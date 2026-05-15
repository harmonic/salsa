use {
    crate::{
        admin_rpc_service,
        commands::{FromClapArgMatches, Result},
    },
    clap::{App, Arg, ArgMatches, SubCommand, value_t},
    solana_clap_utils::{
        input_parsers::pubkey_of,
        input_validators::{is_keypair, is_pubkey_or_keypair},
    },
    solana_keypair::read_keypair,
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{fs, path::Path},
};

const COMMAND: &str = "set-identity-and-vote-account";

/// Identity argument that reads the keypair from stdin, so it need not be on disk
const STDIN: &str = "-";

#[derive(Debug, PartialEq)]
pub struct SetIdentityAndVoteAccountArgs {
    pub identity: String,
    pub vote_account: Pubkey,
    pub require_tower: bool,
    pub require_vote_history: bool,
}

impl FromClapArgMatches for SetIdentityAndVoteAccountArgs {
    fn from_clap_arg_match(matches: &ArgMatches) -> Result<Self> {
        Ok(SetIdentityAndVoteAccountArgs {
            identity: value_t!(matches, "identity", String).map_err(|e| {
                clap::Error::with_description(&e.to_string(), clap::ErrorKind::ValueValidation)
            })?,
            vote_account: pubkey_of(matches, "vote_account").ok_or_else(|| {
                clap::Error::with_description(
                    "vote_account must be a pubkey or a keypair file",
                    clap::ErrorKind::ValueValidation,
                )
            })?,
            require_tower: matches.is_present("require_tower"),
            require_vote_history: !matches.is_present("do_not_require_vote_history"),
        })
    }
}

fn is_keypair_or_stdin(string: String) -> std::result::Result<(), String> {
    if string == STDIN {
        Ok(())
    } else {
        is_keypair(string)
    }
}

pub fn command<'a>() -> App<'a, 'a> {
    SubCommand::with_name(COMMAND)
        .about("Set the validator identity and vote account")
        .arg(
            Arg::with_name("identity")
                .index(1)
                .required(true)
                .value_name("KEYPAIR")
                .validator(is_keypair_or_stdin)
                .help("Path to validator identity keypair, or - to read a JSON keypair from stdin"),
        )
        .arg(
            Arg::with_name("vote_account")
                .index(2)
                .required(true)
                .value_name("ADDRESS")
                .validator(is_pubkey_or_keypair)
                .help("Validator vote account public key"),
        )
        .arg(
            clap::Arg::with_name("require_tower")
                .long("require-tower")
                .takes_value(false)
                .help("Refuse to set the validator identity if saved tower state is not found"),
        )
        .arg(
            clap::Arg::with_name("do_not_require_vote_history")
                .long("do-not-require-vote-history")
                .takes_value(false)
                .help("Do not require saved vote history state for identity change"),
        )
        .after_help(
            "Note: the new identity and vote account only apply to the currently running \
             validator instance",
        )
}

pub fn execute(matches: &ArgMatches, ledger_path: &Path) -> Result<()> {
    let SetIdentityAndVoteAccountArgs {
        identity,
        vote_account,
        require_tower,
        require_vote_history,
    } = SetIdentityAndVoteAccountArgs::from_clap_arg_match(matches)?;

    let vote_account_str = vote_account.to_string();

    if identity == STDIN {
        let identity_keypair = read_keypair(&mut std::io::stdin())?;

        println!("New validator identity: {}", identity_keypair.pubkey());
        println!("New vote account: {vote_account_str}");

        let admin_client = admin_rpc_service::connect(ledger_path);
        admin_rpc_service::runtime().block_on(async move {
            admin_client
                .await?
                .set_identity_and_vote_account_from_bytes(
                    Vec::from(identity_keypair.to_bytes()),
                    vote_account_str,
                    require_tower,
                    require_vote_history,
                )
                .await
        })?;
    } else {
        let identity_keypair = fs::canonicalize(&identity)?;

        println!(
            "New validator identity path: {}",
            identity_keypair.display()
        );
        println!("New vote account: {vote_account_str}");

        let admin_client = admin_rpc_service::connect(ledger_path);
        admin_rpc_service::runtime().block_on(async move {
            admin_client
                .await?
                .set_identity_and_vote_account(
                    identity_keypair.display().to_string(),
                    vote_account_str,
                    require_tower,
                    require_vote_history,
                )
                .await
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::commands::tests::{
            verify_args_struct_by_command, verify_args_struct_by_command_is_error,
        },
        solana_keypair::Keypair,
    };

    #[test]
    fn verify_args_struct_by_command_with_stdin_identity() {
        let vote_account = Keypair::new().pubkey();
        let vote_account_str = vote_account.to_string();

        verify_args_struct_by_command(
            command(),
            vec![COMMAND, STDIN, &vote_account_str],
            SetIdentityAndVoteAccountArgs {
                identity: STDIN.to_string(),
                vote_account,
                require_tower: false,
                require_vote_history: true,
            },
        );
    }

    #[test]
    fn verify_args_struct_by_command_with_pubkey_string() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file = tmp_dir.path().join("id.json");
        let keypair = Keypair::new();
        solana_keypair::write_keypair_file(&keypair, &file).unwrap();

        let vote_account = Keypair::new();
        let vote_account_str = vote_account.pubkey().to_string();

        verify_args_struct_by_command(
            command(),
            vec![COMMAND, file.to_str().unwrap(), &vote_account_str],
            SetIdentityAndVoteAccountArgs {
                identity: file.to_str().unwrap().to_string(),
                vote_account: vote_account.pubkey(),
                require_tower: false,
                require_vote_history: true,
            },
        );
    }

    #[test]
    fn verify_args_struct_by_command_with_keypair_file() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let id_file = tmp_dir.path().join("id.json");
        let keypair = Keypair::new();
        solana_keypair::write_keypair_file(&keypair, &id_file).unwrap();

        let vote_keypair = Keypair::new();
        let vote_file = tmp_dir.path().join("vote.json");
        solana_keypair::write_keypair_file(&vote_keypair, &vote_file).unwrap();

        verify_args_struct_by_command(
            command(),
            vec![
                COMMAND,
                id_file.to_str().unwrap(),
                vote_file.to_str().unwrap(),
            ],
            SetIdentityAndVoteAccountArgs {
                identity: id_file.to_str().unwrap().to_string(),
                vote_account: vote_keypair.pubkey(),
                require_tower: false,
                require_vote_history: true,
            },
        );
    }

    #[test]
    fn verify_args_struct_by_command_with_require_tower() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file = tmp_dir.path().join("id.json");
        let keypair = Keypair::new();
        solana_keypair::write_keypair_file(&keypair, &file).unwrap();

        let vote_account = Keypair::new();
        let vote_account_str = vote_account.pubkey().to_string();

        verify_args_struct_by_command(
            command(),
            vec![
                COMMAND,
                file.to_str().unwrap(),
                &vote_account_str,
                "--require-tower",
            ],
            SetIdentityAndVoteAccountArgs {
                identity: file.to_str().unwrap().to_string(),
                vote_account: vote_account.pubkey(),
                require_tower: true,
                require_vote_history: true,
            },
        );
    }

    #[test]
    fn verify_args_struct_by_command_with_do_not_require_vote_history() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file = tmp_dir.path().join("id.json");
        let keypair = Keypair::new();
        solana_keypair::write_keypair_file(&keypair, &file).unwrap();

        let vote_account = Keypair::new().pubkey();
        let vote_account_str = vote_account.to_string();

        verify_args_struct_by_command(
            command(),
            vec![
                COMMAND,
                file.to_str().unwrap(),
                &vote_account_str,
                "--do-not-require-vote-history",
            ],
            SetIdentityAndVoteAccountArgs {
                identity: file.to_str().unwrap().to_string(),
                vote_account,
                require_tower: false,
                require_vote_history: false,
            },
        );
    }

    #[test]
    fn verify_args_struct_by_command_rejects_invalid_vote_account() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file = tmp_dir.path().join("id.json");
        solana_keypair::write_keypair_file(&Keypair::new(), &file).unwrap();

        verify_args_struct_by_command_is_error::<SetIdentityAndVoteAccountArgs>(
            command(),
            vec![COMMAND, file.to_str().unwrap(), "not-a-pubkey-or-file"],
        );
    }
}
