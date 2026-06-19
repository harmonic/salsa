# Co-located backrun stream

This patch lets a Harmonic validator stream its **own** pre-execution
transactions to a co-located strategy server, and include the backrun bundles
that server sends back in the block — but **only while this validator is the
leader**. It does not involve Harmonic's block builder or any external mempool;
the data is the validator's own transaction ingress.

The feature is **opt-in**. A validator that updates to this build but does not
pass `--backrun-listen-addr` behaves exactly as before.

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
