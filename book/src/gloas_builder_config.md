# Builder Configuration

> This applies from the **Gloas** fork onwards. It configures how the validator client sources
> execution-payload bids from external builders under ePBS.

The validator client reads its external-builder settings from a YAML file named
`builder_definitions.yml` in the validator directory
(`<datadir>/validators/builder_definitions.yml`). The file contains:

- **A global bid policy** — `min_bid` and `builder_boost_factor`, applied to bids received over p2p
  (gossip) and used as the default for any builder that does not set its own.
- **A list of builders** to request bids from directly, each with optional per-builder overrides of
  the global policy.
- **Per-validator configurations** under `validator_configs`, managed through the standard keymanager
  API. Each map key is a validator public key.

Use `GET`, `POST`, and `DELETE` at `/eth/v1/validator/{pubkey}/builder_config`. `GET` returns the
configuration in use. `POST` replaces the stored configuration. `DELETE` restores global inheritance.

## Moving from mev-boost

From the Gloas fork, the validator client sends its list of builders to the beacon node. The beacon
node requests bids from these builders directly. These settings have no effect from Gloas:

- The beacon node `--builder` flag and mev-boost.
- The beacon node `--builder-header-timeout` flag.
- The beacon node `--builder-fallback-epochs-since-finalization` flag.
- The validator client `--builder-proposals` flag.
- The validator client `--builder-registration-timestamp-override` and
  `--validator-registration-batch-size` flags. The validator client does not send validator
  registrations from Gloas.
- The `builder_proposals`, `prefer_builder_proposals` and `builder_boost_factor` fields in
  `validator_definitions.yml`. The same fields in the validator client `PATCH`
  `/lighthouse/validators/{pubkey}` endpoint, and the same `lighthouse vm` flags.

Use the CLI flags below, `builder_definitions.yml` or the keymanager `builder_config` API instead.
If Gloas is scheduled, the beacon node logs a warning at startup when `--builder` is set. The
validator client logs a warning when `validator_definitions.yml` has one of these fields. It also
logs a warning when builder proposals are enabled and no builders are configured.

These settings still apply from Gloas:

- The beacon node `--builder-user-agent` and `--builder-disable-ssz` flags. They do not need
  `--builder`.
- The validator client `--gas-limit` flag and the `gas_limit` field in `validator_definitions.yml`.
  From Gloas, the value is the target gas limit in the proposer preferences.

To use external builders from Gloas, set `--builder-urls` or the `builders` list in
`builder_definitions.yml`. Without builders, the beacon node still compares bids received over gossip
with the local payload.

## CLI flags

You can set the global values with CLI flags instead of the file:

| Flag | File field | Meaning |
| ------ | ------------ | --------- |
| `--builder-urls` | `builders` | Comma-separated builder URLs. Replaces the `builders` list in the file. |
| `--builder-min-bid` | `min_bid` | Minimum total payment, in gwei. A bid below this value ranks behind the local payload. |
| `--builder-boost-factor` | `builder_boost_factor` | Boost factor, in percent. `100` is neutral. The same flag also applies before Gloas. |
| `--prefer-builder-proposals` | — | From Gloas, sets the boost factor to its maximum. |
| `--builder-max-execution-payment` | `max_execution_payment` | Cap, in gwei, for each builder that does not set its own cap. Default is `0`. |

All payment values are in gwei. 1 ETH is `1000000000` gwei. If a cap is more than 10 ETH, the
validator client logs a warning at startup.

```bash
lighthouse vc \
  --builder-urls "https://builder-a.example.com,https://builder-b.example.com#0x0123" \
  --builder-min-bid 500000000 \
  --builder-max-execution-payment 1000000000
```

Most builders do not need `auth_data`. If a builder gives you a value, append it to the URL as a `#0x`
hex fragment. Without a fragment, `auth_data` is the hostname of the URL.

> **Quote the URLs.** In a shell, an `.env` file or a Docker Compose file, a `#` can start a comment.
> An unquoted `#0x0123` can then be removed without an error.

### Precedence

If you set `--builder-urls`, the order of precedence for each value is:

1. The configuration for one validator (`validator_configs` and the keymanager API).
2. The CLI flag.
3. The global value in `builder_definitions.yml`.
4. The default.

If you do not set `--builder-urls`, the validator client uses the `builders` list in the file. The
order of precedence for each value is:

1. The configuration for one validator (`validator_configs` and the keymanager API).
2. The `min_bid`, `builder_boost_factor` and `max_execution_payment` of a builder in the file.
3. The CLI flag.
4. The global value in `builder_definitions.yml`.
5. The default.

A builder in the global list without `max_execution_payment` takes the value of
`--builder-max-execution-payment`, else `0`. A builder in the configuration for one validator takes
its `max_execution_payment` from the builder with the same URL in the global list. If no builder
matches, it takes the value of `--builder-max-execution-payment`, else `0`.

The validator client keeps the CLI values in memory. It never writes them to `builder_definitions.yml`.
The keymanager `GET` returns the values in use, including the CLI values. If a client sends these
values back with `POST`, they are stored for that validator, and later changes to the CLI flags do not
apply to it.

At startup, the validator client logs a warning for each CLI flag that overrides a different value in
the file. It also logs a warning for each value of a builder in the file that takes precedence over a
CLI flag. Then it logs the global values in use, the source of each value, and each builder.

The validator client validates `builder_definitions.yml` at startup, also when `--builder-urls`
replaces its `builders` list. If the file is not valid, the validator client does not start.

`--builder-boost-factor` conflicts with `--prefer-builder-proposals`. From Gloas,
`--prefer-builder-proposals` makes a builder bid of any value above zero beat the local payload. This
includes bids received over gossip. Use `--builder-min-bid` to set a minimum.

## Example

```yaml
# Global bid policy: applies to p2p (gossip) bids, and is the default for any
# builder below that omits the corresponding field.
min_bid: 0                    # gwei — bids below this rank last; one wins only if nothing else is viable
builder_boost_factor: 100     # percent — 100 = neutral, >100 favors builders, 0 = prefer local

builders:
  # Minimal builder — inherits the global policy.
  - enabled: true
    url: "https://builder-a.example.com"
    max_execution_payment: 1000000000   # gwei — cap on the trusted execution payment

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

### Top level (global bid policy)

| Field | Required | Default | Meaning |
| ------- | ---------- | --------- | --------- |
| `min_bid` | no | `0` | Minimum total payment, in gwei, for a p2p bid. A bid below the floor is ranked behind any floor-clearing candidate (including the local block) and only wins when nothing else is viable. Also the default `min_bid` for any builder that omits it. |
| `builder_boost_factor` | no | `100` | Percentage multiplier applied to p2p bids when comparing against the local block. Also the default for any builder that omits it. |
| `builders` | no | `[]` | The list of builders to request bids from directly. |
| `validator_configs` | no | `{}` | Builder settings for individual validators. Omitted fields use global values. An empty `builders` list uses no direct builders. |

### Per builder (each entry under `builders`)

| Field | Required | Default | Meaning |
| ------- | ---------- | --------- | --------- |
| `enabled` | **yes** | — | Whether this builder is used. Disabled builders are ignored. |
| `url` | **yes** | — | The builder's `http`/`https` URL. Bids are requested from here at block-production time. |
| `max_execution_payment` | no | *(`--builder-max-execution-payment`, else `0`)* | Cap, in gwei, on the *trusted* execution payment accepted from this builder. |
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

At block-production time the beacon node requests a bid from each enabled builder with a `url`.
It also considers bids seen over p2p. For each candidate bid:

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
