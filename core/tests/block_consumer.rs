use {
    agave_reserved_account_keys::ReservedAccountKeys,
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView,
        transaction_view::SanitizedTransactionView,
    },
    crossbeam_channel::unbounded,
    solana_core::{banking_stage::committer::Committer, block_stage::BlockConsumer},
    solana_keypair::Keypair,
    solana_ledger::genesis_utils::{create_genesis_config_with_leader, GenesisConfigInfo},
    solana_poh::{
        record_channels::{record_channels, RecordReceiver},
        transaction_recorder::TransactionRecorder,
    },
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::Bank, bank_forks::BankForks, prioritization_fee_cache::PrioritizationFeeCache,
    },
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_signer::Signer,
    solana_system_transaction::transfer,
    solana_transaction::{sanitized::MessageHash, versioned::VersionedTransaction},
    std::sync::{Arc, RwLock},
};

struct TestFixture {
    genesis_config_info: GenesisConfigInfo,
    bank: Arc<Bank>,
    #[allow(dead_code)]
    bank_forks: Arc<RwLock<BankForks>>,
    block_consumer: BlockConsumer,
    #[allow(dead_code)]
    record_receiver: RecordReceiver,
}

impl TestFixture {
    fn new() -> Self {
        let leader_keypair = Keypair::new();
        let genesis_config_info =
            create_genesis_config_with_leader(10_000, &leader_keypair.pubkey(), 10_000_000);
        let (bank, bank_forks) =
            Bank::new_with_bank_forks_for_tests(&genesis_config_info.genesis_config);

        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let committer = Committer::new(
            None,
            replay_vote_sender,
            Arc::new(PrioritizationFeeCache::new(0u64)),
        );

        let (record_sender, mut record_receiver) = record_channels(false);
        record_receiver.restart(bank.bank_id());
        let transaction_recorder = TransactionRecorder::new(record_sender);

        let block_consumer = BlockConsumer::new(
            committer,
            transaction_recorder,
            None, // log_messages_bytes_limit
        );

        TestFixture {
            genesis_config_info,
            bank,
            bank_forks,
            block_consumer,
            record_receiver,
        }
    }
}

/// Helper to convert VersionedTransaction bytes to RuntimeTransaction<ResolvedTransactionView>
fn to_runtime_transaction(serialized: &[u8]) -> RuntimeTransaction<ResolvedTransactionView<&[u8]>> {
    let transaction_view = SanitizedTransactionView::try_new_sanitized(serialized, true).unwrap();
    let static_runtime_tx = RuntimeTransaction::<SanitizedTransactionView<_>>::try_from(
        transaction_view,
        MessageHash::Compute,
        None,
    )
    .unwrap();
    RuntimeTransaction::<ResolvedTransactionView<_>>::try_from(
        static_runtime_tx,
        None,
        &ReservedAccountKeys::empty_key_set(),
    )
    .unwrap()
}

#[test]
fn test_block_consumer_executes_transactions() {
    let mut fixture = TestFixture::new();
    let bank = fixture.bank.clone();
    let intended_slot = bank.slot();

    // Create a test transaction - transfer from mint to a new account
    let keypair1 = Keypair::new();

    // Fund keypair1 from the mint (use small amount since mint has 10_000 lamports)
    let transfer_amount = 1_000;
    let transfer1 = transfer(
        &fixture.genesis_config_info.mint_keypair,
        &keypair1.pubkey(),
        transfer_amount,
        bank.last_blockhash(),
    );

    // Serialize the transaction - needs to stay alive for the transaction view
    let serialized = bincode::serialize(&VersionedTransaction::from(transfer1)).unwrap();

    // Convert to RuntimeTransaction<ResolvedTransactionView> using zerocopy parsing
    let transactions = vec![to_runtime_transaction(&serialized)];

    // Create max_ages for the transactions
    let max_ages = vec![
        solana_core::banking_stage::scheduler_messages::MaxAge {
            sanitized_epoch: bank.epoch(),
            alt_invalidation_slot: bank.slot(),
        };
        transactions.len()
    ];

    // Verify initial balance
    assert_eq!(bank.get_balance(&keypair1.pubkey()), 0);

    // Process the block transactions (optimistic recording)
    let output = fixture
        .block_consumer
        .process_and_record_block_transactions(&bank, &transactions, &max_ages, intended_slot);

    // Verify that transactions were processed successfully
    let result = &output
        .execute_and_commit_transactions_output
        .commit_transactions_result;
    if let Err(e) = result {
        panic!("commit_transactions_result failed: {:?}", e);
    }

    let commit_result = output
        .execute_and_commit_transactions_output
        .commit_transactions_result
        .unwrap();
    assert_eq!(commit_result.len(), 1);

    // Verify the transaction was committed successfully
    match &commit_result[0] {
        solana_core::banking_stage::committer::CommitTransactionDetails::Committed {
            result,
            ..
        } => {
            assert!(result.is_ok(), "Transaction should succeed: {:?}", result);
        }
        other => panic!("Expected Committed, got {:?}", other),
    }

    // Verify that the balance was updated
    assert_eq!(bank.get_balance(&keypair1.pubkey()), transfer_amount);
}

#[test]
fn test_block_consumer_with_empty_transactions() {
    let mut fixture = TestFixture::new();
    let bank = fixture.bank.clone();
    let intended_slot = bank.slot();

    let transactions: Vec<RuntimeTransaction<ResolvedTransactionView<&[u8]>>> = vec![];
    let max_ages = vec![];

    let output = fixture
        .block_consumer
        .process_and_record_block_transactions(&bank, &transactions, &max_ages, intended_slot);

    // Should return success for empty transactions
    assert!(output
        .execute_and_commit_transactions_output
        .commit_transactions_result
        .is_ok());
}

#[test]
fn test_block_consumer_with_invalid_transaction() {
    let mut fixture = TestFixture::new();
    let bank = fixture.bank.clone();
    let intended_slot = bank.slot();

    // Create an invalid transaction (insufficient funds)
    let keypair = Keypair::new();
    let invalid_transfer = transfer(
        &keypair, // No funds
        &Pubkey::new_unique(),
        1_000_000,
        bank.last_blockhash(),
    );

    let serialized = bincode::serialize(&VersionedTransaction::from(invalid_transfer)).unwrap();
    let transactions = vec![to_runtime_transaction(&serialized)];
    let max_ages = vec![solana_core::banking_stage::scheduler_messages::MaxAge {
        sanitized_epoch: bank.epoch(),
        alt_invalidation_slot: bank.slot(),
    }];

    let output = fixture
        .block_consumer
        .process_and_record_block_transactions(&bank, &transactions, &max_ages, intended_slot);

    // Should handle invalid transaction gracefully
    assert!(output
        .execute_and_commit_transactions_output
        .commit_transactions_result
        .is_ok());
}
