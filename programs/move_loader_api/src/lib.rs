const MOVE_LOADER_PROGRAM_ID: [u8; 32] = [
    5, 91, 237, 31, 90, 253, 197, 145, 157, 236, 147, 43, 6, 5, 157, 238, 63, 151, 181, 165, 118,
    224, 198, 97, 103, 136, 113, 64, 0, 0, 0, 0,
];

solana_sdk::solana_name_id!(
    MOVE_LOADER_PROGRAM_ID,
    "MvLdr11111111111111111111111111111111111111"
);

use language_e2e_tests::compile_and_execute;
use log::*;
use solana_sdk::account::KeyedAccount;
use solana_sdk::instruction::InstructionError;
use solana_sdk::loader_instruction::LoaderInstruction;
use solana_sdk::pubkey::Pubkey;
use std::str;
use types::transaction::{Program, TransactionArgument};

pub fn process_instruction(
    _program_id: &Pubkey,
    keyed_accounts: &mut [KeyedAccount],
    ix_data: &[u8],
) -> Result<(), InstructionError> {
    solana_logger::setup();

    if let Ok(instruction) = bincode::deserialize(ix_data) {
        match instruction {
            LoaderInstruction::Write { offset, bytes } => {
                if keyed_accounts[0].signer_key().is_none() {
                    warn!("key[0] did not sign the transaction");
                    return Err(InstructionError::GenericError);
                }
                let offset = offset as usize;
                let len = bytes.len();
                debug!("Write: offset={} length={}", offset, len);
                if keyed_accounts[0].account.data.len() < offset + len {
                    warn!(
                        "Write overflow: {} < {}",
                        keyed_accounts[0].account.data.len(),
                        offset + len
                    );
                    return Err(InstructionError::GenericError);
                }
                keyed_accounts[0].account.data[offset..offset + len].copy_from_slice(&bytes);
            }
            LoaderInstruction::Finalize => {
                if keyed_accounts[0].signer_key().is_none() {
                    warn!("key[0] did not sign the transaction");
                    return Err(InstructionError::GenericError);
                }
                keyed_accounts[0].account.executable = true;
                info!(
                    "Finalize: account {:?}",
                    keyed_accounts[0].signer_key().unwrap()
                );
            }
            LoaderInstruction::InvokeMain { data } => {
                if !keyed_accounts[0].account.executable {
                    warn!("Account not executable");
                    return Err(InstructionError::GenericError);
                }

                //// TODO: Check that keyed_accounts[0].owner is the Move program id.

                // TODO: Return an error
                let args: Vec<TransactionArgument> = bincode::deserialize(&data).unwrap();

                let (programs, _params) = keyed_accounts.split_at_mut(1);
                let program: Program = serde_json::from_slice(&programs[0].account.data).unwrap();

                // TODO: Read bytecode, not source code.
                let code = str::from_utf8(&program.code()).unwrap();
                compile_and_execute(&code, args).unwrap().unwrap();

                info!("Call Move program");
            }
        }
    } else {
        warn!("Invalid program transaction: {:?}", ix_data);
        return Err(InstructionError::GenericError);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    //use compiler::Compiler;
    use solana_sdk::account::Account;
    //use types::account_address::AccountAddress;

    #[test]
    fn test_invoke_main() {
        let program_id = Pubkey::new(&MOVE_LOADER_PROGRAM_ID);

        let code = "main() { return; }";

        // TODO: Produce the same bytes as "compiler -o"
        //
        //let address = AccountAddress::default();
        //let compiler = Compiler {
        //    code,
        //    address,
        //    ..Compiler::default()
        //};
        //let compiled_program = compiler.into_compiled_program().expect("Failed to compile");
        //
        //let mut script = vec![];
        //compiled_program
        //    .script
        //    .serialize(&mut script)
        //    .expect("Unable to serialize script");
        //let mut modules = vec![];
        //for m in compiled_program.modules.iter() {
        //    let mut buf = vec![];
        //    m.serialize(&mut buf).expect("Unable to serialize module");
        //    modules.push(buf);
        //}
        let modules = vec![];
        let script = code.as_bytes().to_vec();

        let program = Program::new(script, modules, vec![]);
        let program_bytes = serde_json::to_vec(&program).unwrap();

        let move_program_pubkey = Pubkey::new_rand();
        let mut move_program_account = Account {
            lamports: 1,
            data: program_bytes,
            owner: program_id,
            executable: true,
        };

        let mut keyed_accounts = vec![KeyedAccount::new(
            &move_program_pubkey,
            false,
            &mut move_program_account,
        )];

        let args: Vec<TransactionArgument> = vec![];
        let data = bincode::serialize(&args).unwrap();
        let ix = LoaderInstruction::InvokeMain { data };
        let ix_data = bincode::serialize(&ix).unwrap();

        // Ensure no panic.
        process_instruction(&program_id, &mut keyed_accounts, &ix_data).unwrap();
    }
}
