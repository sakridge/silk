use streamer;
pub fn ecdsa_verify(batches: Vec<SharedPacketData>) -> Vec<Vec<u8>> {
    let mut rvs = Vec::new();
    for packets in batches { 
        println!("Starting verify num packets: {}", trs.len());
        if trs.len() == 0 {
            return;
        }

        let len = trs.len();
        let mut packet_lens: Vec<u32> = vec![0; len];
        let mut packet_offsets: Vec<u32> = vec![0; len];
        let mut message_offsets: Vec<u32> = vec![0; len];
        let mut message_lens: Vec<u32> = vec![0; len];
        let signature_offset: u32 = offset_of!(Transaction, sig) as u32;
        let public_key_offset: u32 = offset_of!(Transaction, from) as u32;
        let num_keys: size_t = trs.len();
        let mut out: Vec<u8> = Vec::with_capacity(len);
        println!("lens: {}", packet_lens.len());
        for (i, _tr) in trs.iter().enumerate() {
            packet_lens[i] = size_of::<Transaction>() as u32;
            packet_offsets[i] = (i * size_of::<Transaction>()) as u32;
            message_offsets[i] = offset_of!(Transaction, plan) as u32;
            message_lens[i] = (offset_of!(Transaction, sig) - offset_of!(Transaction, plan)) as u32;
        }
        println!("Starting verify num packets: {}", trs.len());
        unsafe {
            let res = ed25519_verify_many(
                trs.as_ptr() as *const u8, //std::mem::transmute::<&[Transaction], &[u8]>(trs),
                packet_lens.as_ptr(),
                packet_offsets.as_ptr(),
                message_lens.as_ptr(),
                message_offsets.as_ptr(),
                public_key_offset,
                signature_offset,
                num_keys,
                out.as_mut_ptr(),
            );
            if res != 0 {}
        }
        println!("done verify");
        rvs.push(out);
    }
}
