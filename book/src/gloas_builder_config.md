# Builder Configuration

> The information below applies from the **Gloas** fork and onwards. It describes how to configure the validator client to source
> execution-payload bids from directly connected builders under ePBS.

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
2. *Per-builder* values in the `builders` list, unless the `--builder-urls` flag is set.
3. *Global* values set via CLI flags. Does not overwrite individually configured validators.
4. *Global* values listed outside the `builders` list in `builder_definitions.yml`.
5. *Defaults* `min_bid` `0`, `builder_boost_factor` `100`, `max_execution_payment` `0`.

`builder_definitions.yml` is in the validator directory (`<datadir>/validators/`). For the keymanager
API, `GET` returns the configuration in use. `POST` replaces the configuration for one validator.
`DELETE` removes it, so the validator uses the values from steps 2 to 5 again.

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

At block production, the beacon node requests a bid from each connected builder.
It also uses the bids it received over gossip. Then it applies these rules:

- **`min_bid`.** A bid must be at least `min_bid` in order to be selected above the locally constructed
  payload. A bid below `min_bid` can be used as a backup in case the local build fails.
- **`builder_boost_factor`.** A percentage multiplier applied to the bid value before it is compared
  against the locally constructed payload. For example, `50` uses a bid only if it is worth 2x the
  local payload. `0` always selects the local payload, unless the local build fails.
- **`max_execution_payment`.** See [Execution payments](#execution-payments).
- **`builder_pubkeys`.** If set, the beacon node discards a bid from a directly connected builder
  that is not signed by one of these keys.
