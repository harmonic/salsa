use {
    crossbeam_channel::{unbounded, Receiver},
    solana_account::{state_traits::StateMut, AccountSharedData, WritableAccount},
    solana_bundle::SanitizedBundle,
    solana_core::{
        banking_stage::{committer::CommitTransactionDetails, qos_service::QosService},
        bundle_stage::{
            bundle_account_locker::BundleAccountLocker, bundle_consumer::BundleConsumer,
            committer::Committer,
        },
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        tip_manager::{TipDistributionAccountConfig, TipManager, TipManagerConfig},
    },
    solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
    solana_hash::Hash,
    solana_instruction::{error::InstructionError, AccountMeta, Instruction},
    solana_keypair::Keypair,
    solana_ledger::{
        blockstore::Blockstore,
        genesis_utils::{create_genesis_config_with_leader, GenesisConfigInfo},
        get_tmp_ledger_path_auto_delete,
        leader_schedule_cache::LeaderScheduleCache,
    },
    solana_loader_v3_interface::{
        instruction::UpgradeableLoaderInstruction, state::UpgradeableLoaderState,
    },
    solana_poh::{
        poh_recorder::{PohRecorder, Record, WorkingBankEntry},
        poh_service::PohService,
        transaction_recorder::TransactionRecorder,
    },
    solana_poh_config::PohConfig,
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::Bank, bank_forks::BankForks, installed_scheduler_pool::BankWithScheduler,
        prioritization_fee_cache::PrioritizationFeeCache,
    },
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_sdk_ids::{bpf_loader_upgradeable, system_program},
    solana_signer::Signer,
    solana_streamer::socket::SocketAddrSpace,
    solana_transaction::{sanitized::SanitizedTransaction, Transaction},
    solana_transaction_error::TransactionError,
    std::{
        str::FromStr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        time::Duration,
    },
};

struct TestFixture {
    genesis_config_info: GenesisConfigInfo,
    bank: Arc<Bank>,
    bank_forks: Arc<RwLock<BankForks>>,
    bundle_consumer: BundleConsumer,
    record_receiver: Receiver<Record>,
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

        let tip_manager = TipManager::new(TipManagerConfig {
            tip_payment_program_id: Pubkey::from_str("T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt")
                .unwrap(),
            tip_distribution_program_id: Pubkey::from_str(
                "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7",
            )
            .unwrap(),
            tip_distribution_account_config: TipDistributionAccountConfig {
                merkle_root_upload_authority: Pubkey::new_unique(),
                vote_account: genesis_config_info.voting_keypair.pubkey(),
                commission_bps: 10,
            },
        });

        let block_builder_info = Arc::new(Mutex::new(BlockBuilderFeeInfo {
            block_builder: Pubkey::new_unique(),
            block_builder_commission: 10,
        }));

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new(leader_keypair.pubkey(), 0, 0),
            Arc::new(leader_keypair),
            SocketAddrSpace::new(true),
        ));

        let is_exited = Arc::new(AtomicBool::new(false));
        let (record_sender, record_receiver) = unbounded();
        let transaction_recorder = TransactionRecorder::new(record_sender, is_exited.clone());

        let bundle_consumer = BundleConsumer::new(
            committer,
            transaction_recorder,
            QosService::new(1),
            None,
            tip_manager,
            BundleAccountLocker::default(),
            block_builder_info,
            Duration::from_secs(10),
            cluster_info,
        );

        TestFixture {
            genesis_config_info,
            bank,
            bank_forks,
            bundle_consumer,
            record_receiver,
        }
    }
}

struct TestProgram {
    program_address: Pubkey,
    programdata_address: Pubkey,
    recipient: Pubkey,
    authority: Keypair,
}

impl TestProgram {
    fn new(bank: &Arc<Bank>) -> Self {
        let program_address = Pubkey::new_unique();
        let (programdata_address, _) = Pubkey::find_program_address(
            &[program_address.as_ref()],
            &bpf_loader_upgradeable::id(),
        );
        let recipient = Pubkey::new_unique();
        let authority = Keypair::new();
        let bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../programs/bpf_loader/test_elfs/out/noop_aligned.so"
        ));

        let mut program_account = AccountSharedData::new_data(
            1_000_000,
            &UpgradeableLoaderState::Program {
                programdata_address,
            },
            &bpf_loader_upgradeable::id(),
        )
        .unwrap();
        program_account.set_executable(true);

        let metadata_size = UpgradeableLoaderState::size_of_programdata_metadata();
        let mut programdata_account = AccountSharedData::new(
            1_000_000,
            metadata_size + bytes.len(),
            &bpf_loader_upgradeable::id(),
        );
        programdata_account
            .set_state(&UpgradeableLoaderState::ProgramData {
                slot: bank.slot(),
                upgrade_authority_address: Some(authority.pubkey()),
            })
            .unwrap();
        programdata_account.data_as_mut_slice()[metadata_size..].copy_from_slice(bytes);

        let recipient_account = AccountSharedData::new(1_000_000, 0, &system_program::id());

        bank.store_account(&program_address, &program_account);
        bank.store_account(&programdata_address, &programdata_account);
        bank.store_account(&recipient, &recipient_account);

        Self {
            program_address,
            programdata_address,
            recipient,
            authority,
        }
    }

    fn close_tx(
        &self,
        payer: &Keypair,
        recent_blockhash: Hash,
    ) -> RuntimeTransaction<SanitizedTransaction> {
        let ix = Instruction::new_with_bincode(
            bpf_loader_upgradeable::id(),
            &UpgradeableLoaderInstruction::Close,
            vec![
                AccountMeta::new(self.programdata_address, false),
                AccountMeta::new(self.recipient, false),
                AccountMeta::new_readonly(self.authority.pubkey(), true),
                AccountMeta::new(self.program_address, false),
            ],
        );
        RuntimeTransaction::from_transaction_for_tests(Transaction::new_signed_with_payer(
            &[ix],
            Some(&payer.pubkey()),
            &[&payer, &self.authority],
            recent_blockhash,
        ))
    }

    fn invoke_tx(
        &self,
        payer: &Keypair,
        recent_blockhash: Hash,
    ) -> RuntimeTransaction<SanitizedTransaction> {
        let ix = Instruction::new_with_bytes(
            self.program_address,
            &rand::random::<u64>().to_le_bytes(),
            vec![AccountMeta::new(payer.pubkey(), true)],
        );
        RuntimeTransaction::from_transaction_for_tests(Transaction::new_signed_with_payer(
            &[ix],
            Some(&payer.pubkey()),
            &[payer],
            recent_blockhash,
        ))
    }
}

fn simulate_poh(bank: &Arc<Bank>, record_receiver: Receiver<Record>) -> Receiver<WorkingBankEntry> {
    let is_exited = Arc::new(AtomicBool::new(false));
    let (mut poh_recorder, entry_receiver) = PohRecorder::new(
        bank.tick_height(),
        bank.last_blockhash(),
        bank.clone(),
        Some((4, 4)),
        bank.ticks_per_slot(),
        Arc::new(
            Blockstore::open(get_tmp_ledger_path_auto_delete!().path())
                .expect("Expected to be able to open database ledger"),
        ),
        &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
        &PohConfig::default(),
        is_exited.clone(),
    );
    poh_recorder.set_bank(BankWithScheduler::new_without_scheduler(bank.clone()));

    // Simulate PoH
    let poh_recorder = Arc::new(RwLock::new(poh_recorder));
    std::thread::Builder::new()
        .name("solana-simulate_poh".to_string())
        .spawn(move || loop {
            PohService::read_record_receiver_and_process(
                &poh_recorder,
                &record_receiver,
                Duration::from_millis(10),
            );
            if is_exited.load(Ordering::Relaxed) {
                break;
            }
        })
        .unwrap();
    entry_receiver
}

#[test]
fn test_execute_record_commit_bundle() {
    solana_logger::setup();
    let TestFixture {
        genesis_config_info,
        bank,
        bank_forks,
        mut bundle_consumer,
        record_receiver,
    } = TestFixture::new();

    let test_program = TestProgram::new(&bank);
    bank.freeze();
    let bank = Bank::new_from_parent(bank.clone(), &Pubkey::default(), bank.slot() + 1);
    let bank = bank_forks
        .write()
        .unwrap()
        .insert(bank)
        .clone_without_scheduler();

    // Keep the entry receiver in scope to avoid send errors for PoH
    let _entry_receiver = simulate_poh(&bank, record_receiver);

    // Make bundle
    let payer = &genesis_config_info.mint_keypair;
    let recent_blockhash = bank.last_blockhash();
    let transactions = vec![
        test_program.invoke_tx(payer, recent_blockhash),
        test_program.close_tx(payer, recent_blockhash),
        test_program.invoke_tx(payer, recent_blockhash),
    ];
    let sanitized_bundle = SanitizedBundle {
        transactions,
        slot: bank.slot(),
    };

    // Execute bundle
    let result = BundleConsumer::execute_record_commit_bundle(
        &bundle_consumer.committer,
        &bundle_consumer.transaction_recorder,
        &bundle_consumer.log_messages_bytes_limit,
        bundle_consumer.max_bundle_retry_duration,
        &sanitized_bundle,
        &bank,
        true,
        &mut bundle_consumer.scheduler,
        &bundle_consumer.thread_pool,
    );
    // Print results
    eprintln!("Result: {:?}", result.result);
    for (i, details) in result.commit_transaction_details.iter().enumerate() {
        match details {
            CommitTransactionDetails::Committed { result, .. } => {
                eprintln!("  TXN #{i}: Committed {{ {result:?} }}");
            }
            CommitTransactionDetails::NotCommitted(e) => {
                eprintln!("  TXN #{i}: NotCommitted {{ {e:?} }}");
            }
        }
    }
    // Block execution should have succeeded
    assert!(result.result.is_ok());
    // The first invoke should have succeeded
    assert!(matches!(
        result.commit_transaction_details[0],
        CommitTransactionDetails::Committed { result: Ok(()), .. }
    ));
    // The close should have succeeded
    assert!(matches!(
        result.commit_transaction_details[1],
        CommitTransactionDetails::Committed { result: Ok(()), .. }
    ));
    // The second invoke should fail because the program is now closed
    assert!(matches!(
        result.commit_transaction_details[2],
        CommitTransactionDetails::Committed {
            result: Err(TransactionError::InstructionError(
                _,
                InstructionError::UnsupportedProgramId
            )),
            ..
        }
    ));
}
