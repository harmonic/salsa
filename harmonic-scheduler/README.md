# Harmonic Scheduler

Harmonic transaction scheduler for the Agave validator.

See [docs.harmonic.gg](https://docs.harmonic.gg) for more details.

## Building

```bash
cargo build --release -p harmonic-scheduler
```

Binary: `target/release/harmonic-scheduler`.

## Running

The validator must be started with `--enable-scheduler-bindings` and `--shred-receiver-address <ADDR>`. Endpoints for `--shred-receiver-address`, `--remote-tpu-url`, and `--block-engine-url` are listed at [docs.harmonic.gg/run-a-validator/endpoints](https://docs.harmonic.gg/run-a-validator/endpoints).

```bash
harmonic-scheduler \
  --ledger /path/to/ledger \
  --remote-tpu-url <REMOTE_TPU_URL> \
  --block-engine-url <BLOCK_ENGINE_URL> \
  --tip-payment-program-pubkey <PUBKEY> \
  --tip-distribution-program-pubkey <PUBKEY> \
  --merkle-root-upload-authority <PUBKEY>
```

The scheduler reads `admin.rpc` and `scheduler_bindings.ipc` from `--ledger`,
and pulls the validator's identity keypair and vote account from the admin RPC.
Running `agave-validator set-identity` is propagated automatically.

### Required arguments

| Flag | Description |
|------|-------------|
| `--ledger` | Validator ledger directory |
| `--remote-tpu-url` | Remote TPU gRPC endpoint |
| `--block-engine-url` | Block engine gRPC endpoint |
| `--tip-payment-program-pubkey` | Tip payment program |
| `--tip-distribution-program-pubkey` | Tip distribution program |
| `--merkle-root-upload-authority` | Merkle-root upload authority |

### Optional arguments

| Flag | Default | Description |
|------|---------|-------------|
| `--num-workers` | 8 | Worker thread count |
| `--commission-bps` | 0 | Validator tip commission (bps) |
| `--strategy` | `fba` | Block builder strategy: `fifo`, `fba`, `mrev` |
| `--log` / `-o` | — | Log file (SIGUSR1 to rotate) |
| `--backrun-listen-addr` | — | Serve the co-located backrun stream (see [BACKRUN.md](BACKRUN.md)) |
| `--backrun-x-token` | — | Optional shared secret required from backrun clients |
