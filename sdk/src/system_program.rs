solana_keypair::declare_id!("11111111111111111111111111111111");

pub fn solana_system_program() -> (String, solana_keypair::pubkey::Pubkey) {
    ("solana_system_program".to_string(), id())
}
