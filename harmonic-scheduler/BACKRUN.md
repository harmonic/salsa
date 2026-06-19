# Co-located backrun stream

This patch lets a Harmonic validator stream its **own** pre-execution
transactions to a co-located strategy server, and include the backrun bundles
that server sends back in the block — but **only while this validator is the
leader**. It does not involve Harmonic's block builder or any external mempool;
the data is the validator's own transaction ingress.

The feature is **opt-in**. A validator that updates to this build but does not
pass `--backrun-listen-addr` behaves exactly as before.

## Updating an existing 4.0.3 validator

This patch changes **only the `harmonic-scheduler` crate**. The agave validator
binary, `scheduler-bindings`, the IPC surface, and on-disk state are all
unchanged, so updating is just swapping the scheduler binary:

1. Check out this branch and rebuild only the scheduler:
   ```bash
   cargo build --release -p harmonic-scheduler
   ```
2. Replace your running `harmonic-scheduler` binary with the new
   `target/release/harmonic-scheduler`.
3. Restart the scheduler, adding `--backrun-listen-addr` (and optionally
   `--backrun-x-token`). Every other flag stays the same.

No change to the agave validator process, no re-sync, no gossip/config changes.
Rolling back is the same step in reverse (old binary, drop the flag).

## Connection model

By gRPC role the **validator is the server** and your **strategy box is the
client** — even though the strategy is the one "listening" for transactions.
This follows the `BackrunService` contract: the side that *has* the
transactions streams them out (`SubscribeBackruns`, server→client) and *accepts*
bundles (`SendBundle`, client→server). The validator has the transactions, so it
hosts the server; the strategy dials in.

The connection is **persistent**:

- The server is started once at process startup and listens for the whole
  lifetime of the scheduler, independent of its IPC reconnect loop.
- The client opens a single long-lived `SubscribeBackruns` stream that is
  **never closed based on leader state**. While not leader it carries only 5s
  keepalive pings; while leader it carries transactions. So the stream stays
  warm across slots — there is no per-slot connect/handshake latency.
- Streaming is gated on leadership *at the data level only* (the `is_leader`
  flag), not at the connection level.

The strategy should treat the stream as long-lived and **reconnect on drop**
(e.g. validator restart). The `searcher-client-example` client exits when the
stream ends; for an always-on searcher, wrap its connect + subscribe loop in a
retry.

## How it works

```
            leader slot only
  validator TPU ─┐
                 ├─► nonvote ingress ──► (stream out) ──► strategy server
  relayer ───────┘                                         │  SubscribeBackruns
                                                           ▼
  block being built  ◄──── (inject) ◄──── SendBundle ◄──── strategy server
```

- **Stream out** (`validator/tpu.rs`): the scheduler already funnels every
  incoming non-vote transaction — both the validator's direct TPU stream
  (`tpu_to_pack`) and its relayer-forwarded packets — through one point. While
  the `is_leader` flag is set, each such transaction's raw bytes are mirrored to
  the `BackrunService` subscribers. Votes are not streamed.
- **Leader gating** (`validator/scheduler.rs`): the scheduler sets `is_leader`
  true on entering a leader slot and false otherwise. Outside the leader slot,
  nothing is streamed.
- **Inject back** (`validator/scheduler.rs`): backrun bundles received over
  `SendBundle` are buffered and, during `block_stage`/`fallback_stage`, allocated
  into shared memory and ticked into the block exactly like the tip bundle —
  so they execute and land in the leader's block.

The gRPC server (`backrun.rs`) implements the same `BackrunService` that
`searcher-client-example-master` already speaks, so the strategy server connects
as the client with no changes.

## Running

Start the scheduler with the backrun endpoint enabled:

```bash
harmonic-scheduler \
  --ledger /path/to/ledger \
  --remote-tpu-url <REMOTE_TPU_URL> \
  --block-engine-url <BLOCK_ENGINE_URL> \
  --tip-payment-program-pubkey <PUBKEY> \
  --tip-distribution-program-pubkey <PUBKEY> \
  --merkle-root-upload-authority <PUBKEY> \
  --backrun-listen-addr 0.0.0.0:50051        # enables the feature
  # --backrun-x-token <SECRET>               # optional shared secret
```

The strategy server (co-located, same datacenter) connects with the
`searcher-client-example` client:

```bash
backrun_grpc_client --grpc-url http://<validator-host>:50051 --x-token <SECRET>
```

It receives every non-vote transaction the validator sees during its leader
slots, and returns backrun bundles via `SendBundle` that the validator includes
in the block.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--backrun-listen-addr` | unset (disabled) | Address to serve the backrun gRPC stream on |
| `--backrun-x-token` | none | If set, required in the `x-token` metadata of clients |

## Notes / limitations

- Streaming and injection are active **only during this validator's leader
  slots**. A backrun must arrive before the slot's block-building window closes
  to be included; the co-located searcher has the leader slot to react.
- Bundles are included in arrival/lock order via the normal scheduler tick.
  Strict "immediately after a specific trigger tx" ordering is up to the bundle
  contents (e.g. the searcher sends `[target, backrun]`); the scheduler executes
  the bundle's transactions in order, subject to account-lock scheduling.
- No revert protection: a backrun that no longer makes sense simply fails and
  costs fees, as with any backrun.
