# Proposer

The proposer client is implemented as a minimal diff on top of the jito client,
where a whole block is treated as a large bundle with several additional properties.
Firstly, this bundle does not have revert protect; it is allowed to have failed transactions.
Secondly, only one bundle is allowed to execute per slot.

The initial (current) implementation intended to allow blockbuilders to submit the full block
with votes included. For several reasons, such as external optics (regarding vote censorship)
and bandwidth, we have decided to revert this and give the proposer a vote CU budget that they
must use by tick 38 (6 * ticks_per_slot / 10 for tick_per_slot=64). The vast majority of votes
are presently submitted about a quarter way through the next block, and this hard cutoff enables
the builders to be informed via turbine the remaining CU budget for the remaining portion of the block.

We would like to enable a fallback mechanism for when the proposer does not receive a block in time,
whether due to the block auction house being down, or because the builders are all offline. After some
threshold (presently 48 ticks but definitely not final), the proposer stops waiting for a block and
begins sprinting a block locally. This rqeuires that the two schedulers (in bundle_stage, banking_stage)
have a synchronization mechanism. This is implemented simply as an atomic counter which each scheduler
attempt to update to the current slot via cmpxchg. Early in the block, during the time a block is expected,
the bundle_stage attempts to update the counter AFTER receiving a block. If successful, the block is executed.
If execution fails, then the counter is reverted and the banking stage scheduler proceeds. If execution succeeds, then the banking stage scheduler remains stalled and the bundle block is recorded, shredded, and
broadcasted to the network.

Overall, the philisophy of the system is as follows. So long as there is no slashing on solana, minimal verification needs to be done to the block. An invalid block can make it through this system which will
be rejected by the network. Any blockbuilder that submits a provably invalid block will simply be penalized with either a temporary or permanent ban from the auction house. However, this QoS mechanism is outside of the
scope of the proposer client.



## Additional semi-relevant information
The proposer can share their tpu flow with builders via the same relayer mechanism that the jito client presently has. There will be public relayers that share tpu flow with registered, KYC'd builders that
connect to the transaction relayer.