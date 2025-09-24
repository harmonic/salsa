use criterion::{
    criterion_group, criterion_main, AxisScale, BenchmarkId, Criterion, PlotConfiguration,
    Throughput,
};
use rand::prelude::*;
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use serde::{Deserialize, Serialize};
use solana_bundle::bundle_execution::{load_and_execute_bundle, parallel_load_and_execute_bundle};
use solana_bundle::scheduler::Scheduler;
use solana_bundle::SanitizedBundle;
use solana_clock::MAX_PROCESSING_AGE;
use solana_cluster_type::ClusterType;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_fee_calculator::{FeeRateGovernor, DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE};
use solana_hash::Hash;
use solana_instruction::{AccountMeta, Instruction};
use solana_keypair::Keypair;
use solana_ledger::genesis_utils::GenesisConfigInfo;
use solana_message::Message;
use solana_native_token::LAMPORTS_PER_SOL;
use solana_program_test::programs::spl_programs;
use solana_pubkey::Pubkey;
use solana_rent::Rent;
use solana_runtime::bank::test_utils::deposit;
use solana_runtime::bank::Bank;
use solana_runtime::bank_forks::BankForks;
use solana_runtime::genesis_utils::create_genesis_config_with_leader_ex;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_signer::Signer;
use solana_transaction::sanitized::MessageHash;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction::Transaction;
use solana_vote_interface::state::VoteState;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

const NUM_THREADS: usize = 8;
const MAX_DURATION: Duration = Duration::from_secs(60);
const NUM_ACCOUNTS: usize = 10_000_000;
// const NUM_TRANSACTIONS: [usize; 1] = [1];
// const NUM_TRANSACTIONS: [usize; 1] = [10];
// const NUM_TRANSACTIONS: [usize; 1] = [100];
// const NUM_TRANSACTIONS: [usize; 1] = [1_000];
// const NUM_TRANSACTIONS: [usize; 1] = [10_000];
// const NUM_TRANSACTIONS: [usize; 1] = [100_000];
// const NUM_TRANSACTIONS: [usize; 1] = [1_000_000];
const NUM_TRANSACTIONS: [usize; 5] = [1, 10, 100, 1_000, 10_000];
// const NUM_TRANSACTIONS: [usize; 6] = [1, 10, 100, 1_000, 10_000, 100_000];
// const NUM_TRANSACTIONS: [usize; 7] = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000];

// ***************************************************************************
// Utilities
// ***************************************************************************

fn make_config() -> (GenesisConfigInfo, Arc<Bank>, Arc<RwLock<BankForks>>) {
    let mint_keypair = Keypair::new();
    let leader_keypair = Keypair::new();
    let voting_keypair = Keypair::new();
    let validator_pubkey = leader_keypair.pubkey();
    let rent = Rent::default();
    let mut genesis_config = create_genesis_config_with_leader_ex(
        100 * LAMPORTS_PER_SOL,
        &mint_keypair.pubkey(),
        &validator_pubkey,
        &voting_keypair.pubkey(),
        &Pubkey::new_unique(),
        rent.minimum_balance(VoteState::size_of()) + (LAMPORTS_PER_SOL * 1_000_000),
        LAMPORTS_PER_SOL * 1_000_000,
        FeeRateGovernor {
            // Initialize with a non-zero fee
            lamports_per_signature: DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE / 2,
            ..FeeRateGovernor::default()
        },
        rent.clone(), // most tests don't expect rent
        ClusterType::Development,
        spl_programs(&rent),
    );
    genesis_config.ticks_per_slot *= 8;

    let (bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
    let bank = Arc::new(Bank::new_from_parent(bank, &Pubkey::default(), 1));

    (
        GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            voting_keypair,
            validator_pubkey,
        },
        bank,
        bank_forks,
    )
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Account {
    pub key: [u8; 32],
    pub write: bool,
}

fn parse_transactions(
    path: PathBuf,
    bank: &Bank,
    recent_blockhash: Hash,
    cheap: bool,
    thread_pool: &ThreadPool,
) -> Vec<Transaction> {
    thread_pool.scope(|_| {
        let compute_budget = ComputeBudgetInstruction::set_compute_unit_limit(u32::MAX);
        let program_id = Pubkey::from_str_const("14MQtomES11yENzeomDHWZEgEtA5AxCMncJhjERPQtWD");

        let file = File::open(path.clone()).unwrap();
        let accounts = bincode::deserialize_from::<_, Vec<Vec<Account>>>(file).unwrap();
        // hack to deduplicate account keys and generate new unique keys for
        // each account, since the versions used in this branch don't support
        // creating a new Keypair from just a secret key.
        let account_map = accounts
            .par_iter()
            .flatten()
            .map(|account| account.key.clone())
            .collect::<HashSet<[u8; 32]>>()
            .into_par_iter()
            .map(|account| (account, Keypair::new()))
            .collect::<HashMap<[u8; 32], Keypair>>();

        accounts
            .into_par_iter()
            .enumerate()
            .map(|(i, accounts)| {
                let mut new_accounts = Vec::with_capacity(accounts.len());
                accounts
                    .par_iter()
                    .take(38) // Only 38 keys fit without a lookup table
                    .map(|account| account_map.get(&account.key).unwrap())
                    .collect_into_vec(&mut new_accounts);
                let mut account_meta = Vec::with_capacity(accounts.len());
                accounts
                    .par_iter()
                    .zip(new_accounts)
                    .map(|(account, keypair)| {
                        if account.write {
                            AccountMeta::new(keypair.pubkey(), false)
                        } else {
                            AccountMeta::new_readonly(keypair.pubkey(), false)
                        }
                    })
                    .collect_into_vec(&mut account_meta);
                let instruction =
                    Instruction::new_with_bytes(program_id, &i.to_be_bytes(), account_meta);
                let payer = account_map.get(&accounts[0].key).unwrap();
                // fund each paying account with 100 SOL
                deposit(bank, &payer.pubkey(), 100 * LAMPORTS_PER_SOL).unwrap();
                let mut message = Message::new(
                    &[compute_budget.clone(), instruction],
                    Some(&payer.pubkey()),
                );
                if cheap {
                    // Clear the instructions to make this a no-op
                    message.instructions.clear();
                }
                Transaction::new(&[payer], message, recent_blockhash)
            })
            .collect()
    })
}

fn make_transactions(
    num_accounts: usize,
    num_transactions: usize,
    bank: &Bank,
    recent_blockhash: Hash,
    cheap: bool,
    thread_pool: &ThreadPool,
) -> Vec<Transaction> {
    thread_pool.scope(|_| {
        let compute_budget = ComputeBudgetInstruction::set_compute_unit_limit(u32::MAX);
        let program_id = Pubkey::from_str_const("14MQtomES11yENzeomDHWZEgEtA5AxCMncJhjERPQtWD");

        // create unique account keys
        let accounts = (0..num_accounts)
            .into_par_iter()
            .map(|_| Keypair::new())
            .collect::<Vec<_>>();

        // create acounts for each key and fund each account with 100 SOL
        accounts.par_iter().for_each(|account| {
            deposit(bank, &account.pubkey(), 100 * LAMPORTS_PER_SOL).unwrap();
        });

        // create expensive transactions
        (0..num_transactions)
            .into_par_iter()
            .map(|i| {
                let mut rng = rand::rng();
                let num_accounts = rng.random_range(1..=64);
                let accounts: Vec<_> = accounts.choose_multiple(&mut rng, num_accounts).collect();
                let account_meta: Vec<_> = accounts
                    .iter()
                    .skip(1) // skip the first account - it is the payer
                    .map(|&account| {
                        // make about 20% of account accesses writes, don't make any accounts signers
                        if rng.random_bool(0.2) {
                            AccountMeta::new(account.pubkey(), false)
                        } else {
                            AccountMeta::new_readonly(account.pubkey(), false)
                        }
                    })
                    .collect();
                // Use unique data value to make each transaction unique, the value itself is unused
                let instruction =
                    Instruction::new_with_bytes(program_id, &i.to_be_bytes(), account_meta);
                let payer = accounts[0];
                let mut message = Message::new(
                    &[compute_budget.clone(), instruction],
                    Some(&payer.pubkey()),
                );
                if cheap {
                    // Clear the instructions to make this a no-op
                    message.instructions.clear();
                }
                Transaction::new(&[payer], message, recent_blockhash)
            })
            .collect()
    })
}

fn make_bundle(txs: &[Transaction], bank: &Bank) -> SanitizedBundle {
    let transactions: Vec<_> = txs
        .iter()
        .map(|tx| {
            let tx = VersionedTransaction::from(tx.clone());
            RuntimeTransaction::try_create(
                tx,
                MessageHash::Compute,
                None,
                bank,
                bank.get_reserved_account_keys(),
            )
            .unwrap()
        })
        .collect();

    SanitizedBundle {
        transactions,
        slot: 0,
    }
}

// ***************************************************************************
// Benchmarks
// ***************************************************************************

fn bench_cheap_transactions(c: &mut Criterion) {
    let (genesis_config_info, bank, _bank_forks) = make_config();
    let mut scheduler = Scheduler::new();
    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .build()
        .unwrap();
    let mut group = c.benchmark_group("cheap_transactions");

    for block in std::fs::read_dir("blocks").unwrap() {
        let path = block.unwrap().path();
        let transactions = parse_transactions(
            path.clone(),
            &bank,
            genesis_config_info.genesis_config.hash(),
            true,
            &thread_pool,
        );
        let bundle = make_bundle(&transactions, &bank);
        let execution_accounts = vec![None; transactions.len()];

        group.throughput(Throughput::Elements(transactions.len() as u64));
        group.bench_function(
            BenchmarkId::new(
                "load_and_execute_bundle",
                format!("{:?}", path.file_name().unwrap()),
            ),
            |b| {
                b.iter(|| {
                    let output = load_and_execute_bundle(
                        &bank,
                        &bundle,
                        MAX_PROCESSING_AGE,
                        &MAX_DURATION,
                        true,
                        &None,
                        false,
                        None,
                        &execution_accounts,
                        &execution_accounts,
                    );
                    output.result.unwrap();
                })
            },
        );

        group.bench_function(
            BenchmarkId::new(
                "parallel_load_and_execute_bundle",
                format!("{:?}", path.file_name().unwrap()),
            ),
            |b| {
                b.iter(|| {
                    let output = parallel_load_and_execute_bundle(
                        &bank,
                        &bundle,
                        MAX_PROCESSING_AGE,
                        &MAX_DURATION,
                        true,
                        &None,
                        false,
                        None,
                        &execution_accounts,
                        &execution_accounts,
                        &mut scheduler,
                        &thread_pool,
                    );
                    output.result.unwrap();
                })
            },
        );
    }
}

fn bench_random_cheap_transactions(c: &mut Criterion) {
    let (genesis_config_info, bank, _bank_forks) = make_config();
    let mut scheduler = Scheduler::new();
    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .build()
        .unwrap();
    let mut group = c.benchmark_group("random_cheap_transactions");
    group.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    let transactions = make_transactions(
        NUM_ACCOUNTS,
        NUM_TRANSACTIONS[NUM_TRANSACTIONS.len() - 1],
        &bank,
        genesis_config_info.genesis_config.hash(),
        true,
        &thread_pool,
    );

    for num_transactions in NUM_TRANSACTIONS {
        group.throughput(Throughput::Elements(transactions.len() as u64));
        let bundle = make_bundle(&transactions[..num_transactions], &bank);
        let execution_accounts = vec![None; num_transactions];

        group.bench_function(
            BenchmarkId::new(format!("load_and_execute_bundle"), num_transactions),
            |b| {
                b.iter(|| {
                    load_and_execute_bundle(
                        &bank,
                        &bundle,
                        MAX_PROCESSING_AGE,
                        &MAX_DURATION,
                        true,
                        &None,
                        false,
                        None,
                        &execution_accounts,
                        &execution_accounts,
                    )
                    .result
                    .unwrap()
                })
            },
        );

        group.bench_function(
            BenchmarkId::new(
                format!("parallel_load_and_execute_bundle"),
                num_transactions,
            ),
            |b| {
                b.iter(|| {
                    let output = parallel_load_and_execute_bundle(
                        &bank,
                        &bundle,
                        MAX_PROCESSING_AGE,
                        &MAX_DURATION,
                        true,
                        &None,
                        false,
                        None,
                        &execution_accounts,
                        &execution_accounts,
                        &mut scheduler,
                        &thread_pool,
                    );
                    output.result.unwrap();
                })
            },
        );
    }
}

fn bench_expensive_transactions(c: &mut Criterion) {
    let (genesis_config_info, bank, _bank_forks) = make_config();
    let mut scheduler = Scheduler::new();
    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .build()
        .unwrap();
    let mut group = c.benchmark_group("expensive_transactions");

    for block in std::fs::read_dir("blocks").unwrap() {
        let path = block.unwrap().path();
        let transactions = parse_transactions(
            path.clone(),
            &bank,
            genesis_config_info.genesis_config.hash(),
            false,
            &thread_pool,
        );
        let bundle = make_bundle(&transactions, &bank);
        let execution_accounts = vec![None; transactions.len()];
        group.throughput(Throughput::Elements(transactions.len() as u64));
        group.bench_function(
            BenchmarkId::new(
                "load_and_execute_bundle",
                format!("{:?}", path.file_name().unwrap()),
            ),
            |b| {
                b.iter(|| {
                    load_and_execute_bundle(
                        &bank,
                        &bundle,
                        MAX_PROCESSING_AGE,
                        &MAX_DURATION,
                        true,
                        &None,
                        false,
                        None,
                        &execution_accounts,
                        &execution_accounts,
                    )
                    .result
                    .unwrap()
                })
            },
        );

        group.bench_function(
            BenchmarkId::new(
                "parallel_load_and_execute_bundle",
                format!("{:?}", path.file_name().unwrap()),
            ),
            |b| {
                b.iter(|| {
                    let output = parallel_load_and_execute_bundle(
                        &bank,
                        &bundle,
                        MAX_PROCESSING_AGE,
                        &MAX_DURATION,
                        true,
                        &None,
                        false,
                        None,
                        &execution_accounts,
                        &execution_accounts,
                        &mut scheduler,
                        &thread_pool,
                    );
                    output.result.unwrap();
                })
            },
        );
    }
}

fn bench_random_expensive_transactions(c: &mut Criterion) {
    let (genesis_config_info, bank, _bank_forks) = make_config();
    let mut scheduler = Scheduler::new();
    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .build()
        .unwrap();
    let mut group = c.benchmark_group("random_expensive_transactions");
    let transactions = make_transactions(
        NUM_ACCOUNTS,
        NUM_TRANSACTIONS[NUM_TRANSACTIONS.len() - 1],
        &bank,
        genesis_config_info.genesis_config.hash(),
        false,
        &thread_pool,
    );

    for num_transactions in NUM_TRANSACTIONS {
        if num_transactions > 1000 {
            break;
        }
        let bundle = make_bundle(&transactions[..num_transactions], &bank);
        let execution_accounts = vec![None; num_transactions];

        group.throughput(Throughput::Elements(transactions.len() as u64));
        group.bench_function(
            BenchmarkId::new(format!("load_and_execute_bundle"), num_transactions),
            |b| {
                b.iter(|| {
                    load_and_execute_bundle(
                        &bank,
                        &bundle,
                        MAX_PROCESSING_AGE,
                        &MAX_DURATION,
                        true,
                        &None,
                        false,
                        None,
                        &execution_accounts,
                        &execution_accounts,
                    )
                    .result
                    .unwrap()
                })
            },
        );

        group.bench_function(
            BenchmarkId::new(
                format!("parallel_load_and_execute_bundle"),
                num_transactions,
            ),
            |b| {
                b.iter(|| {
                    let output = parallel_load_and_execute_bundle(
                        &bank,
                        &bundle,
                        MAX_PROCESSING_AGE,
                        &MAX_DURATION,
                        true,
                        &None,
                        false,
                        None,
                        &execution_accounts,
                        &execution_accounts,
                        &mut scheduler,
                        &thread_pool,
                    );
                    output.result.unwrap();
                })
            },
        );
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().noise_threshold(0.1).warm_up_time(Duration::from_millis(500)).measurement_time(Duration::from_secs(3)).sample_size(10);
    targets =
    bench_cheap_transactions,
    bench_expensive_transactions,
    bench_random_cheap_transactions,
    bench_random_expensive_transactions,
);
criterion_main!(benches);
