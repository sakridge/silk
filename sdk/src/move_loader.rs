pub const BS58_STRING: &str = "MoveLdr111111111111111111111111111111111111";
solana_keypair::declare_id!(BS58_STRING);

pub fn solana_move_loader_program() -> (String, solana_keypair::pubkey::Pubkey) {
    ("solana_move_loader_program".to_string(), id())
}
