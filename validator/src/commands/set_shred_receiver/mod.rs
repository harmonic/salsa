use {
    crate::{
        admin_rpc_service,
        commands::{FromClapArgMatches, Result},
    },
    clap::{App, Arg, ArgMatches, SubCommand},
    std::path::Path,
};

const COMMAND: &str = "set-shred-receiver-address";

#[derive(Debug, PartialEq)]
pub struct SetShredReceiverArgs {
    pub addr: String,
}

impl FromClapArgMatches for SetShredReceiverArgs {
    fn from_clap_arg_match(matches: &ArgMatches) -> Result<Self> {
        Ok(SetShredReceiverArgs {
            addr: matches
                .value_of("shred_receiver_address")
                .expect("shred_receiver_address is required")
                .to_string(),
        })
    }
}

pub fn command<'a>() -> App<'a, 'a> {
    SubCommand::with_name(COMMAND)
        .about("Set shred receiver address(es)")
        .arg(
            Arg::with_name("shred_receiver_address")
                .value_name("HOST:PORT")
                .takes_value(true)
                .required(true)
                .help(
                    "Forward all leader shreds to these addresses. Accepts comma-separated \
                     entries. Hostnames resolve to IPv4 only. Up to 32 unique addresses. Empty \
                     string to disable.",
                ),
        )
}

pub fn execute(matches: &ArgMatches, ledger_path: &Path) -> Result<()> {
    let args = SetShredReceiverArgs::from_clap_arg_match(matches)?;

    let admin_client = admin_rpc_service::connect(ledger_path);
    admin_rpc_service::runtime().block_on(async move {
        admin_client
            .await?
            .set_shred_receiver_address(args.addr)
            .await
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::commands::tests::{
            verify_args_struct_by_command, verify_args_struct_by_command_is_error,
        },
    };

    #[test]
    fn verify_args_struct_by_command_default() {
        verify_args_struct_by_command_is_error::<SetShredReceiverArgs>(command(), vec![COMMAND]);
    }

    #[test]
    fn verify_args_struct_by_command_with_addr() {
        verify_args_struct_by_command(
            command(),
            vec![COMMAND, "127.0.0.1:9001"],
            SetShredReceiverArgs {
                addr: "127.0.0.1:9001".to_string(),
            },
        );
    }
}
