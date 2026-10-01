use feature_dispatch::feature_dispatch;
use std::marker::PhantomData;

trait Feature {}

struct Toy;

impl Feature for Toy {}

struct Active<F: Feature>(PhantomData<F>);

struct Spec {
    toy_fork_epoch: u64,
}

impl Spec {
    fn feature_enabled<F: Feature>(&self, epoch: u64) -> Option<Active<F>> {
        (epoch >= self.toy_fork_epoch).then_some(Active(PhantomData))
    }
}

mod toy {
    use super::*;

    pub fn reward(base: u64, _spec: &Spec, _epoch: u64, _active: Active<Toy>) -> u64 {
        base * 2
    }

    pub fn push(counter: &mut Counter, value: u64, _active: Active<Toy>) {
        counter.values.push(value * 10);
    }

    pub fn total(counter: &Counter, _active: Active<Toy>) -> u64 {
        counter.values.iter().sum::<u64>() + 1
    }
}

#[feature_dispatch(Toy => toy::reward, spec = spec, epoch = epoch)]
fn reward(base: u64, spec: &Spec, epoch: u64) -> u64 {
    base
}

#[feature_dispatch(Toy => toy::reward, spec = spec, epoch = epoch)]
fn reward_with_early_return(base: u64, spec: &Spec, epoch: u64) -> u64 {
    if base == 0 {
        return 7;
    }
    base + 1
}

struct Counter {
    spec: Spec,
    epoch: u64,
    values: Vec<u64>,
}

impl Counter {
    #[feature_dispatch(Toy => toy::push, spec = self.spec, epoch = self.epoch)]
    fn push(&mut self, value: u64) {
        self.values.push(value);
    }

    #[feature_dispatch(Toy => toy::total, spec = &self.spec, epoch = self.epoch)]
    fn total(&self) -> u64 {
        self.values.iter().sum()
    }
}

#[test]
fn free_function_runs_the_original_body_before_the_fork() {
    let spec = Spec { toy_fork_epoch: 10 };
    assert_eq!(reward(5, &spec, 9), 5);
    assert_eq!(reward_with_early_return(0, &spec, 9), 7);
    assert_eq!(reward_with_early_return(5, &spec, 9), 6);
}

#[test]
fn free_function_calls_the_copy_from_the_fork() {
    let spec = Spec { toy_fork_epoch: 10 };
    assert_eq!(reward(5, &spec, 10), 10);
    assert_eq!(reward_with_early_return(0, &spec, 11), 0);
}

#[test]
fn methods_pass_self_to_the_copy() {
    let mut counter = Counter {
        spec: Spec { toy_fork_epoch: 1 },
        epoch: 0,
        values: vec![],
    };
    counter.push(1);
    assert_eq!(counter.total(), 1);

    counter.epoch = 1;
    counter.push(2);
    assert_eq!(counter.values, [1, 20]);
    assert_eq!(counter.total(), 22);
}

#[test]
fn an_argument_named_active_is_passed_through() {
    fn copy(active: bool, _token: Active<Toy>) -> bool {
        !active
    }

    #[feature_dispatch(Toy => copy, spec = Spec { toy_fork_epoch: 0 }, epoch = 0)]
    fn original(active: bool) -> bool {
        active
    }

    assert!(!original(true));
}
