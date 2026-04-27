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
  --validator-socket /path/to/ledger/scheduler_bindings.ipc \
  --remote-tpu-url <REMOTE_TPU_URL> \
  --block-engine-url <BLOCK_ENGINE_URL> \
  --identity /path/to/identity.json \
  --tip-payment-program-pubkey <PUBKEY> \
  --tip-distribution-program-pubkey <PUBKEY> \
  --vote-account <PUBKEY> \
  --merkle-root-upload-authority <PUBKEY>
```

### Required arguments

| Flag | Description |
|------|-------------|
| `--validator-socket` | Validator IPC socket path |
| `--remote-tpu-url` | Remote TPU gRPC endpoint |
| `--block-engine-url` | Block engine gRPC endpoint |
| `--identity` | Validator identity keypair |
| `--tip-payment-program-pubkey` | Tip payment program |
| `--tip-distribution-program-pubkey` | Tip distribution program |
| `--vote-account` | Validator vote account |
| `--merkle-root-upload-authority` | Merkle-root upload authority |

### Optional arguments

| Flag | Default | Description |
|------|---------|-------------|
| `--num-workers` | 8 | Worker thread count |
| `--commission-bps` | 0 | Validator tip commission (bps) |
| `--strategy` | `fba` | Block builder strategy: `fifo`, `fba`, `mrev` |
| `--log` / `-o` | — | Log file (SIGUSR1 to rotate) |
