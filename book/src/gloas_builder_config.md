# Builder Configuration

> This applies from the **Gloas** fork onwards. It configures how the validator client sources
> execution-payload bids from external builders under ePBS.

Enshrined Proposer-Builder Separation (ePBS) adds the builder role as a new type of protocol
participant. A builder builds the execution payload for a block and offers it to proposers in the
form of a bid.

When a validator proposes a block, the beacon node collects bids from two sources:

- **Gossip.** Builders publish bids on the `execution_payload_bid` p2p topic.
- **Direct builders.** The beacon node requests a bid from each builder that the validator client
  configured as a directly connected builder.

The beacon node compares the highest value bid against its locally constructed payload and selects
the highest value option. See [How bids are selected](#how-bids-are-selected).

## Configuration options

The validator client sets builder configuration in the following order of precedence:

1. *Per-validator* configurations via the keymanager API or `validator_configs`.
2. *Per-builder* values in the `builders` list, for bids from that builder only, unless
   `--builder-urls` is set.
3. *Global* values set via CLI flags.
4. *Global* `min_bid` and `builder_boost_factor` outside the `builders` list in
   `builder_definitions.yml`.
5. *Defaults* `min_bid` `0`, `builder_boost_factor` `100`, `max_execution_payment` `0`.

Use `GET`, `POST`, and `DELETE` at `/eth/v1/validator/{pubkey}/builder_config`. `GET` returns the
configuration in use. `POST` replaces the stored configuration. `DELETE` restores global inheritance.

## Direct builder connections

Post Gloas, the validator client sends its list of builders to the beacon node. The beacon
node requests bids from these builders directly.

To set this list, use `--builder-urls` or the `builders` list in `builder_definitions.yml`.
Without builders, the beacon node still compares bids received over gossip against the locally constructed payload.

## CLI flags

| Flag | File field | Meaning |
| ------ | ------------ | --------- |
| `--builder-urls` | `builders` | Comma-separated builder URLs. Replaces the `builders` list in the file. |
| `--builder-min-bid` | `min_bid` | Minimum total payment, in gwei. A bid below this value ranks behind the local payload. |
| `--builder-boost-factor` | `builder_boost_factor` | Boost factor, in percent. `100` is neutral. |
| `--prefer-builder-proposals` | — | Post Gloas, sets the boost factor to its maximum. |
| `--builder-max-execution-payment` | `max_execution_payment` | Cap, in gwei, for each builder that does not set its own cap. Default is `0`. |

All payment values are in gwei. 1 ETH is `1000000000` gwei.

```bash
lighthouse vc \
  --builder-urls "https://builder-a.example.com,https://builder-b.example.com#0x0123" \
  --builder-min-bid 500000000 \
  --builder-max-execution-payment 1000000000
```

The validator client signs each bid request with the validator key. The signed message contains
the slot and `auth_data`, a value that identifies the builder. The builder checks `auth_data` to
make sure that the request is for this builder.

By default, `auth_data` is the hostname of the URL. If a builder provides a value, append it to the URL as a `#0x` hex fragment.

### Execution payments

A bid from a builder has two payments:

- The bid `value`. This payment is trustless, the protocol enforces this.
- The `execution_payment`. This payment is trusted, the builder promises to pay it outside the protocol. Nothing on-chain enforces
  this. `execution_payment` is only relevant for bids sourced from directly connected builders.
  Bids received over gossip have no `execution_payment`.

`max_execution_payment` is the largest execution payment that the beacon node considers as part of the bid
value. From the beacon nodes perspective a bid is worth `value + min(execution_payment, max_execution_payment)`. For example, if
`max_execution_payment` is 1 ETH and a builder promises 3 ETH, the beacon node counts 1 ETH. The
default `max_execution_payment` is `0`.

## Example

```yaml
# Global values: apply to gossip bids, and to each builder below that does
# not set its own value.
min_bid: 0                    # gwei — a bid below this wins only if nothing else is available
builder_boost_factor: 100     # percent — 100 = neutral, >100 favors builders, 0 = prefer local

builders:
  # Minimal builder — inherits the global policy.
  - enabled: true
    url: "https://builder-a.example.com"
    max_execution_payment: 1000000000   # gwei — see "Execution payments"

  # Builder overriding the globals and pinning the expected builder key.
  - enabled: true
    url: "https://builder-b.example.com"
    max_execution_payment: 1000000000
    min_bid: 500000000                  # override the global for this builder
    builder_boost_factor: 120           # override the global for this builder
    builder_pubkeys:                    # optional — reject a bid not signed by one of these keys
      - "0xa1b2c3d4..."
    # auth_data: "0x6275696c..."        # optional — defaults to the hostname of `url`

# Optional per-validator configuration.
# validator_configs:
#   "0x<validator-public-key>":
#     min_bid: 500000000
#     builders: []                 # explicitly disable direct builders for this validator
```

> **Comments are not preserved.** The validator client rewrites this file when builder settings
> change through the keymanager API. Keep an annotated copy elsewhere if you rely on inline notes.

## Fields

### Global values

| Field | Required | Default | Meaning |
| ------- | ---------- | --------- | --------- |
| `min_bid` | no | `0` | Minimum total payment, in gwei. A bid below this value ranks behind the locally constructed payload, and wins only if nothing else is available. Also the default for each builder that does not set its own `min_bid`. |
| `builder_boost_factor` | no | `100` | Percentage multiplier applied to a bid value before it is compared against the locally constructed payload. Also the default for each builder that does not set its own `builder_boost_factor`. |
| `builders` | no | `[]` | The list of builders to request bids from directly. |
| `validator_configs` | no | `{}` | Builder settings for individual validators. Omitted fields use global values. An empty `builders` list uses no direct builders. |

### Per builder (each entry under `builders`)

| Field | Required | Default | Meaning |
| ------- | ---------- | --------- | --------- |
| `enabled` | **yes** | — | Whether this builder is used. Disabled builders are ignored. |
| `url` | **yes** | — | The builder's `http`/`https` URL. Bids are requested from here at block-production time. |
| `max_execution_payment` | no | *(`--builder-max-execution-payment`, else `0`)* | Cap, in gwei, on the execution payment counted from this builder. See [Execution payments](#execution-payments). |
| `min_bid` | no | *(global)* | Override the global minimum bid for this builder. |
| `builder_boost_factor` | no | *(global)* | Override the global boost factor for this builder. |
| `builder_pubkeys` | no | *(empty)* | The builder's BLS public keys, hex-encoded. If non-empty, a returned bid **not** signed by one of them is rejected. |
| `auth_data` | no | *(hostname of `url`)* | Opaque authentication data, hex-encoded, agreed with the builder out of band. Signed into the request. Must be non-empty when set. Defaults to the lowercase ASCII hostname of `url`. |

All byte fields (`builder_pubkeys` entries, `auth_data`) are `0x`-prefixed hex strings. All payment values
(`min_bid`, `max_execution_payment`) are in gwei.

The default `auth_data` excludes the scheme, credentials, port, path, query and fragment of the URL.
For example, both `https://builder.example.com` and `https://builder.example.com/` use
`builder.example.com`. An internationalized hostname must use punycode. IPv6 addresses use compressed,
bracketed hexadecimal form, such as `[::1]` or `[::ffff:c000:201]`.

A builder that uses a different identity must agree explicit `auth_data` with the validator operator.
Explicit values are signed exactly as configured. See the
[default authentication data specification](https://github.com/ethereum/builder-specs/pull/168).

## How bids are selected

At block-production time the validator client requests a bid from each enabled builder with a `url`,
and also considers bids seen over p2p. For each candidate bid:

- **`min_bid`** — a bid whose total value is below the applicable `min_bid` is ranked behind any
  floor-clearing candidate (including the local block) rather than dropped, so it wins only when
  nothing else is viable (e.g. the local build failed). Direct builders use their own (or the
  inherited per-validator) value; p2p bids use the validator's `min_bid`.
- **`builder_boost_factor`** — the surviving bid's value is scaled by its boost factor
  (`boost × value ÷ 100`) before being compared against the locally-built block. A factor below
  `100` favors the local block; above `100` favors the builder; `0` always prefers local;
  `2^64 − 1` strongly favors the builder. The factor is a multiplier, not an absolute override, so a
  zero-value bid still ranks `0` and loses to any non-zero local block.
- **`max_execution_payment`** — bounds how much of a builder's (off-chain) execution payment counts
  toward its bid value. This applies only to direct builders; p2p bids carry no trusted execution
  payment.
- **`builder_pubkeys`** — for a direct builder, if non-empty, the returned bid must be signed by
  one of these keys or it is discarded.

The highest-value bid after these rules wins. Per-builder `min_bid`/`builder_boost_factor` apply
only to bids requested directly by URL. For p2p bids, per-validator defaults override the global values.
