pub mod earliest_deadline_scheduler;

use futures::stream::Stream;
use futures::StreamExt;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use task_executor::TaskExecutor;
use tokio::sync::mpsc::{self, Receiver, Sender};

/// The name of the worker tokio tasks.
const WORKER_TASK_NAME: &str = "beacon_processor_worker";

/// The name of the manager tokio task.
const MANAGER_TASK_NAME: &str = "earliest_deadline_first_scheduler";

/// Unique IDs used for metrics and testing.
pub const WORKER_FREED: &str = "worker_freed";
pub const NOTHING_TO_DO: &str = "nothing_to_do";

/// The maximum size of the channel for idle events to the `BeaconProcessor`.
///
/// Setting this too low will prevent new workers from being spawned. It *should* only need to be
/// set to the CPU count, but we set it high to be safe.
const MAX_IDLE_QUEUE_LEN: usize = 16_384;

pub type AsyncFn = Pin<Box<dyn Future<Output = ()> + Send + Sync>>;
pub type BlockingFn = Box<dyn FnOnce() + Send + Sync>;

pub trait Clock: Send + Sync + Sized + Clone {}

pub enum ProcessingType {
    Async(AsyncFn),
    Blocking(BlockingFn),
    BlockingOrAsync(BlockingOrAsync),
}

pub enum BlockingOrAsync {
    Blocking(BlockingFn),
    Async(AsyncFn),
}

/// A mutli-threaded processor for messages received on the network
/// that need to be processed
pub struct BeaconProcessor<C> {
    pub executor: TaskExecutor,
    pub current_workers: usize,
    pub max_workers: usize,
    pub config: C,
}

/// Spawns tasks that are either:
///
/// - Blocking (i.e. intensive methods that shouldn't run on the core `tokio` executor)
/// - Async (i.e. `async` methods)
///
/// Takes a `SendOnDrop` and ensures it is dropped after the task completes. This frees the beacon
/// processor worker so a new task can be started.
struct TaskSpawner {
    executor: TaskExecutor,
    send_idle_on_drop: SendOnDrop,
}

impl TaskSpawner {
    /// Spawn an async task, dropping the `SendOnDrop` after the task has completed.
    fn spawn_async(self, task: impl Future<Output = ()> + Send + 'static) {
        self.executor.spawn(
            async {
                task.await;
                drop(self.send_idle_on_drop)
            },
            WORKER_TASK_NAME,
        )
    }

    /// Spawn a blocking task, dropping the `SendOnDrop` after the task has completed.
    fn spawn_blocking<F>(self, task: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.executor.spawn_blocking(
            || {
                task();
                drop(self.send_idle_on_drop)
            },
            WORKER_TASK_NAME,
        )
    }
}

pub trait GenericWork: Send + Sync {
    fn work_type_str(&self) -> &'static str;

    fn work_type<Y>(&self) -> Y;

    fn processing_type(&self) -> ProcessingType;

    fn is_priority_work(&self) -> bool;

    fn reprocess_work<R>(&self) -> Option<R>;

    fn drop_under_global_condition(&self) -> bool;

    fn calculate_deadline<T>(&self, clock: T) -> Option<Duration>;
}

pub struct GenericWorkEvent<W: GenericWork> {
    pub drop_during_sync: bool,
    pub work: W,
}

/// Combines the various incoming event streams for the `BeaconProcessor` into a single stream.
///
/// This struct has a similar purpose to `tokio::select!`, however it allows for more fine-grained
/// control (specifically in the ordering of event processing).
struct InboundEvents<W: GenericWork, R> {
    /// Used by workers when they finish a task.
    idle_rx: mpsc::Receiver<()>,
    /// Used by upstream processes to send new work to the `BeaconProcessor`.
    event_rx: mpsc::Receiver<GenericWorkEvent<W>>,
    /// Used by the reprocess queue to send new work to the `BeaconProcessor`
    /// when its deemed ready to be reprocessed.
    ready_work_rx: mpsc::Receiver<R>,
}

pub enum NextGenericWorkEvent<W: GenericWork> {
    GenericWorkEvent(Option<GenericWorkEvent<W>>),
    Continue,
    Break,
}

/// Unifies all the messages processed by the `BeaconProcessor`.
enum InboundEvent<W: GenericWork> {
    /// A worker has completed a task and is free.
    WorkerIdle,
    /// There is new work to be done.
    GenericWorkEvent(GenericWorkEvent<W>),
    /// A work event that was queued for re-processing has become ready.
    ReprocessingWork(GenericWorkEvent<W>),
}

impl<W: GenericWork, R> Stream for InboundEvents<W, R>
where
    GenericWorkEvent<W>: std::convert::From<R>,
{
    type Item = InboundEvent<W>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Always check for idle workers before anything else. This allows us to ensure that a big
        // stream of new events doesn't suppress the processing of existing events.
        match self.idle_rx.poll_recv(cx) {
            Poll::Ready(Some(())) => {
                return Poll::Ready(Some(InboundEvent::WorkerIdle));
            }
            Poll::Ready(None) => {
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }

        // Poll for delayed blocks before polling for new work. It might be the case that a delayed
        // block is required to successfully process some new work.
        match self.ready_work_rx.poll_recv(cx) {
            Poll::Ready(Some(ready_work)) => {
                return Poll::Ready(Some(InboundEvent::ReprocessingWork(ready_work.into())));
            }
            Poll::Ready(None) => {
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }

        match self.event_rx.poll_recv(cx) {
            Poll::Ready(Some(event)) => {
                return Poll::Ready(Some(InboundEvent::GenericWorkEvent(event)));
            }
            Poll::Ready(None) => {
                return Poll::Ready(None);
            }
            Poll::Pending => {}
        }

        Poll::Pending
    }
}

impl<W: GenericWork, R> InboundEvents<W, R>
where
    GenericWorkEvent<W>: std::convert::From<R>,
    R: Send + Sync,
{
    pub async fn next_work_event<C, P>(
        &mut self,
        reprocess_work_tx: &Sender<P>,
        beacon_processor: &mut BeaconProcessor<C>,
    ) -> NextGenericWorkEvent<W>
    where
        P: Send + Sync,
    {
        // TODO
        let enable_backfill_rate_limiting = true;
        match self.next().await {
            Some(InboundEvent::WorkerIdle) => {
                beacon_processor.current_workers =
                    beacon_processor.current_workers.saturating_sub(1);
                NextGenericWorkEvent::GenericWorkEvent(None)
            }
            Some(InboundEvent::GenericWorkEvent(_))
                // we want the ability to introduce some logic somewhere
                // to process/not process work events based on some configuration
                // i.e. backfill rate limiting
                if enable_backfill_rate_limiting =>
                // if beacon_processor.config.enable_backfill_rate_limiting =>
            {
                // generalize backfill rate limiting logic
                todo!()
                // match QueuedBackfillBatch::try_from(event) {
                //     Ok(backfill_batch) => {
                //         match reprocess_work_tx
                //             .try_send(ReprocessQueueMessage::BackfillSync(backfill_batch))
                //         {
                //             Err(e) => {
                //                 warn!(
                //                     beacon_processor.log,
                //                     "Unable to queue backfill work event. Will try to process now.";
                //                     "error" => %e
                //                 );
                //                 match e {
                //                     TrySendError::Full(reprocess_queue_message)
                //                     | TrySendError::Closed(reprocess_queue_message) => {
                //                         match reprocess_queue_message {
                //                             ReprocessQueueMessage::BackfillSync(backfill_batch) => {
                //                                 NextGenericWorkEvent::GenericWorkEvent(Some(
                //                                     backfill_batch.into(),
                //                                 ))
                //                             }
                //                             other => {
                //                                 crit!(
                //                                     beacon_processor.log,
                //                                     "Unexpected queue message type";
                //                                     "message_type" => other.as_ref()
                //                                 );
                //                                 // This is an unhandled exception, drop the message.
                //                                 NextGenericWorkEvent::Continue
                //                             }
                //                         }
                //                     }
                //                 }
                //             }
                //             Ok(..) => {
                //                 // backfill work sent to "reprocessing" queue. Process the next event.
                //                 NextGenericWorkEvent::Continue
                //             }
                //         }
                //     }
                //     Err(event) => NextGenericWorkEvent::GenericWorkEvent(Some(event.into())),
                // }
            }
            Some(InboundEvent::GenericWorkEvent(event)) | Some(InboundEvent::ReprocessingWork(event)) => {
                NextGenericWorkEvent::GenericWorkEvent(Some(event))
            }
            None => {
                // TODO(logging)
                // debug!(
                //     beacon_processor.log,
                //     "Gossip processor stopped";
                //     "msg" => "stream ended"
                // );
                NextGenericWorkEvent::Break
            }
        }
    }
}

/// Spawns a blocking worker thread to process some `Work`.
///
/// Sends an message on `idle_tx` when the work is complete and the task is stopping.
pub fn spawn_worker<W: GenericWork, C>(
    beacon_processor: &mut BeaconProcessor<C>,
    idle_tx: Sender<()>,
    work: W,
) {
    // let work_type = work.work_type_str();
    // TODO add metrics here?

    // Wrap the `idle_tx` in a struct that will fire the idle message whenever it is dropped.
    //
    // This helps ensure that the worker is always freed in the case of an early exit or panic.
    // As such, this instantiation should happen as early in the function as possible.
    let send_idle_on_drop = SendOnDrop {
        tx: idle_tx,
        // _worker_timer: worker_timer,
    };

    // let worker_id = beacon_processor.current_workers;
    beacon_processor.current_workers = beacon_processor.current_workers.saturating_add(1);

    let executor = beacon_processor.executor.clone();

    let task_spawner = TaskSpawner {
        executor,
        send_idle_on_drop,
    };

    match work.processing_type() {
        ProcessingType::Async(pin) => task_spawner.spawn_async(pin),
        ProcessingType::Blocking(fn_once) => task_spawner.spawn_blocking(fn_once),
        ProcessingType::BlockingOrAsync(blocking_or_async) => match blocking_or_async {
            BlockingOrAsync::Blocking(fn_once) => task_spawner.spawn_blocking(fn_once),
            BlockingOrAsync::Async(pin) => task_spawner.spawn_async(pin),
        },
    }
}

/// This struct will send a message on `self.tx` when it is dropped. An error will be logged on
/// `self.log` if the send fails (this happens when the node is shutting down).
///
/// ## Purpose
///
/// This is useful for ensuring that a worker-freed message is still sent if a worker panics.
///
/// The Rust docs for `Drop` state that `Drop` is called during an unwind in a panic:
///
/// https://doc.rust-lang.org/std/ops/trait.Drop.html#panics
pub struct SendOnDrop {
    tx: mpsc::Sender<()>,
    // The field is unused, but it's here to ensure the timer is dropped once the task has finished.
    // _worker_timer: Option<metrics::HistogramTimer>,
}

impl Drop for SendOnDrop {
    fn drop(&mut self) {
        if let Err(e) = self.tx.try_send(()) {
            // log a message
            // warn!(
            //     self.log,
            //     "Unable to free worker";
            //     "msg" => "did not free worker, shutdown may be underway",
            //     "error" => %e
            // )
        }
    }
}

pub struct QueueItem<W: GenericWork> {
    deadline: Duration,
    pub work_event: GenericWorkEvent<W>,
}

impl<W: GenericWork> QueueItem<W> {
    pub fn new<T>(work_event: GenericWorkEvent<W>, clock: T) -> Option<Self> {
        let Some(deadline) = work_event.work.calculate_deadline(clock) else {
            return None;
        };

        Some(Self {
            work_event,
            deadline,
        })
    }
}

impl<W: GenericWork> std::cmp::Eq for QueueItem<W> {}

impl<W: GenericWork> std::cmp::PartialEq for QueueItem<W> {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}

impl<W: GenericWork> std::cmp::PartialOrd for QueueItem<W> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<W: GenericWork> Ord for QueueItem<W> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deadline.cmp(&other.deadline)
    }
}

pub struct WorkQueue<Q: std::cmp::Ord> {
    min_heap: BinaryHeap<Reverse<Q>>,
}

impl<Q: std::cmp::Ord> WorkQueue<Q> {
    pub fn new() -> Self {
        WorkQueue {
            min_heap: BinaryHeap::new(),
        }
    }

    pub fn pop(&mut self) -> Option<Q> {
        if let Some(item) = self.min_heap.pop() {
            return Some(item.0);
        };

        None
    }

    pub fn insert(&mut self, queue_item: Q) {
        self.min_heap.push(Reverse(queue_item))
    }
}

pub struct Scheduler<C, W: GenericWork, S: Clock> {
    beacon_processor: BeaconProcessor<C>,
    work_queue: WorkQueue<QueueItem<W>>,
    clock: S,
}

impl<C: Send + Sync + 'static, W: GenericWork + 'static, S: Clock + 'static> Scheduler<C, W, S> {
    pub fn new(beacon_processor: BeaconProcessor<C>, clock: S) -> Self {
        let work_queue = WorkQueue::new();
        Scheduler {
            beacon_processor,
            work_queue,
            clock,
        }
    }

    // R: ReadyWork
    // P: ReprocessWork
    // T: WorkType
    // G: GlobalState
    pub fn run<R, P, T, G>(
        mut self,
        global_state: std::sync::Arc<G>,
        event_rx: mpsc::Receiver<GenericWorkEvent<W>>,
        work_journal_tx: Option<Sender<&'static str>>,
        f: impl Fn(&std::sync::Arc<G>) -> bool + Send + Sync + 'static,
    ) -> Result<(), String>
    where
        GenericWorkEvent<W>: std::convert::From<R>,
        G: Send + Sync + 'static,
        R: Send + Sync + 'static,
        P: Send + Sync + 'static,
    {
        let (idle_tx, idle_rx) = mpsc::channel::<()>(MAX_IDLE_QUEUE_LEN);

        let (ready_work_tx, ready_work_rx) = mpsc::channel::<R>(self.beacon_processor.max_workers);

        let (reprocess_work_tx, reprocess_work_rx) =
            mpsc::channel::<P>(self.beacon_processor.max_workers);

        let executor = self.beacon_processor.executor.clone();

        let mut inbound_events = InboundEvents {
            idle_rx,
            event_rx,
            ready_work_rx,
        };

        spawn_reprocess_scheduler(ready_work_tx, reprocess_work_rx, &executor)?;

        let manager_future = async move {
            loop {
                let work_event = match inbound_events
                    .next_work_event(&reprocess_work_tx, &mut self.beacon_processor)
                    .await
                {
                    NextGenericWorkEvent::GenericWorkEvent(work_event) => work_event,
                    NextGenericWorkEvent::Continue => continue,
                    NextGenericWorkEvent::Break => break,
                };

                let can_spawn =
                    self.beacon_processor.current_workers < self.beacon_processor.max_workers;

                worker_journal(&work_event, &work_journal_tx);

                let modified_work_id = match work_event {
                    // There is no new work event, but we are able to spawn a new worker.
                    // We don't check the `work.drop_during_sync` here. We assume that if it made
                    // it into the queue at any point then we should process it.
                    None if can_spawn => {
                        if let Some(queue_item) = self.work_queue.pop() {
                            self.process_or_queue_item::<P, T>(
                                &reprocess_work_tx,
                                &idle_tx,
                                queue_item,
                                can_spawn,
                            )
                        } else {
                            // Let the journal know that a worker is freed and there's nothing else
                            // for it to do.
                            if let Some(work_journal_tx) = &work_journal_tx {
                                // We don't care if this message was successfully sent, we only use the journal
                                // during testing.
                                let _ = work_journal_tx.try_send(NOTHING_TO_DO);
                            }
                            None
                        }
                    }
                    // There is no new work event and we are unable to spawn a new worker.
                    //
                    // I cannot see any good reason why this would happen.
                    None => {
                        // TODO log
                        // warn!(
                        //     self.beacon_processor.log,
                        //     "Unexpected gossip processor condition";
                        //     "msg" => "no new work and cannot spawn worker"
                        // );
                        None
                    }
                    // the work event should be dropped due to some global state
                    Some(work_event)
                        if f(&global_state) && work_event.work.drop_under_global_condition() =>
                    {
                        let _work_type = work_event.work.work_type_str();
                        // TODO
                        // metrics::inc_counter_vec(
                        //     &metrics::BEACON_PROCESSOR_WORK_EVENTS_IGNORED_COUNT,
                        //     &[work_id],
                        // );
                        // trace!(
                        //     self.beacon_processor.log,
                        //     "Gossip processor skipping work";
                        //     "msg" => "chain is syncing",
                        //     "work_id" => work_id
                        // );
                        None
                    }
                    Some(work_event) => {
                        if let Some(queue_item) = QueueItem::new(work_event, &self.clock) {
                            self.process_or_queue_item(
                                &reprocess_work_tx,
                                &idle_tx,
                                queue_item,
                                can_spawn,
                            )
                        } else {
                            None
                        }
                    }
                };
            }
        };

        // Spawn on the core executor.
        executor.spawn(manager_future, MANAGER_TASK_NAME);

        Ok(())
    }

    pub fn process_or_queue_item<P, T>(
        &mut self,
        reprocess_work_tx: &Sender<P>,
        idle_tx: &Sender<()>,
        queue_item: QueueItem<W>,
        can_spawn: bool,
    ) -> Option<T> {
        let work_type: T = queue_item.work_event.work.work_type();

        let workers_available =
            self.beacon_processor.max_workers - self.beacon_processor.current_workers;

        if let Some(reprocess_work) = queue_item.work_event.work.reprocess_work::<P>() {
            if let Err(e) = reprocess_work_tx.try_send(reprocess_work) {
                // TODO log error
                // error!(
                //     self.beacon_processor.log,
                //     "Failed to reprocess work event";
                //     "error" => %e
                // )
            }
            return Some(work_type);
        }

        if can_spawn {
            if (queue_item.work_event.work.is_priority_work() && workers_available > 0)
                || workers_available > 1
            {
                spawn_worker(
                    &mut self.beacon_processor,
                    idle_tx.clone(),
                    queue_item.work_event.work,
                )
            } else {
                self.work_queue.insert(queue_item);
            }

            return Some(work_type);
        }

        self.work_queue.insert(queue_item);

        Some(work_type)
    }
}

/// Starts the job that manages scheduling works that need re-processing. The returned `Sender`
/// gives the communicating channel to receive those works. Once a work is ready, it is sent back
/// via `ready_work_tx`.
pub fn spawn_reprocess_scheduler<R, P>(
    ready_work_tx: Sender<R>,
    reprocess_work_rx: Receiver<P>,
    executor: &TaskExecutor,
) -> Result<(), String> {
    todo!()
}

fn worker_journal<W: GenericWork>(
    work_event: &Option<GenericWorkEvent<W>>,
    work_journal_tx: &Option<Sender<&'static str>>,
) {
    if let Some(work_journal_tx) = work_journal_tx {
        let id = work_event
            .as_ref()
            .map(|event| event.work.work_type_str())
            .unwrap_or(WORKER_FREED);

        // We don't care if this message was successfully sent, we only use the journal
        // during testing. We also ignore reprocess messages to ensure our test cases can pass.
        if id != "reprocess" {
            let _ = work_journal_tx.try_send(id);
        }
    }
}
