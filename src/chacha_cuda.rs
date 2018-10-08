use ledger::{LedgerWindow};
use storage_stage::ENTRIES_PER_SLICE;
use std::io;

#[link(name = "cuda-crypt")]
extern "C" {
    fn chacha20_cbc_encrypt_many(
        input: *const u8,
        output: *mut u8,
        in_len: usize,
        keys: *const u8,
        num_keys: u32,
        time_us: *mut f32,
    );
}

pub fn chacha_cbc_encrypt_file_many_keys(in_path: &str, slice: u64, keys: &[u8], samples: &[u64]) -> io::Result<()> {
    let mut ledger_window = LedgerWindow::open(in_path)?;
    let mut buffer = [0; 4 * 1024];
    let mut encrypted = [0; 4 * 1024];
    let mut entry = slice;
    let mut total_entries = 0;
    let mut time: f32 = 0.0;
    while let Ok((num_entries, entry_len)) = ledger_window.get_entries_bytes(entry, ENTRIES_PER_SLICE - total_entries, &mut buffer) {
        total_entries += num_entries;
        entry += num_entries;
        if (entry - slice) > ENTRIES_PER_SLICE {
            break;
        }

        let entry_len_usz = entry_len as usize;
        unsafe {
            chacha20_cbc_encrypt_many(buffer[..entry_len_usz].as_ptr(),
                encrypted.as_mut_ptr(),
                entry_len_usz,
                keys.as_ptr(),
                keys.len() as u32,
                &mut time);
        }
    }
    Ok(())
}


