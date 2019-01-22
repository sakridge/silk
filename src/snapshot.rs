use crate::bank::Bank;
use crate::result::Result;
use bincode::deserialize;
use bincode::serialize;
use solana_sdk::hash::Hash;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::mem::size_of;
use std::sync::Arc;

pub fn create_snapshot(bank: &Arc<Bank>, entry_id: Hash, entry_height: u64) -> Result<()> {
    let mut snapshot_file = File::create("bank.snapshot")?;

    let id = serialize(&entry_id).unwrap();
    snapshot_file.write(&id)?;
    let height = serialize(&entry_height).unwrap();
    snapshot_file.write(&height)?;

    let v = bank.serialize();
    snapshot_file.write(&v)?;

    Ok(())
}

pub fn load_from_snapshot() -> Result<(Bank, u64, Hash)> {
    let bytes = fs::read("bank.snapshot")?;
    info!("loading bank from snapshot");
    let mut cur = size_of::<Hash>();
    let last_entry_id = deserialize(&bytes[..cur])?;
    let entry_height = deserialize(&bytes[cur..cur + 8])?;
    cur += 8;
    let bank = Bank::new_from_snapshot(&bytes[cur..]);
    Ok((bank, entry_height, last_entry_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mint::Mint;
    use solana_sdk::hash::{hash, Hash};
    use solana_sdk::signature::{Keypair, KeypairUtil};

    #[test]
    fn test_snapshot() {
        let mint = Mint::new(1000);
        let bob = Keypair::new();
        let entry_height = 42;
        let bank = Bank::new(&mint);
        let hash0 = Hash::default();
        let id = hash(&hash0.as_ref());
        create_snapshot(&Arc::new(bank), id, entry_height);
    }
}
