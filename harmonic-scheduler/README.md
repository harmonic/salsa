# Harmonic Scheduler

External transaction scheduler for the Agave validator.
See [docs.harmonic.gg](https://docs.harmonic.gg) for setup.

## Building

`scripts/cargo-install-all.sh` builds it with `agave-validator`.

To build it alone:

```bash
cargo build --release --package harmonic-scheduler
```

## Running

Run `harmonic-scheduler --help` for all options.
