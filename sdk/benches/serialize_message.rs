#![feature(test)]

extern crate test;
use bincode::{deserialize, serialize};
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use test::Bencher;

fn make_message() -> Message {
    let meta = AccountMeta::new(Pubkey::new_rand(), false);
    let inst = Instruction::new(Pubkey::new_rand(), &[0; 20], vec![meta; 10]);
    let instructions = vec![inst; 8];
    Message::new(&instructions, None)
}

#[bench]
fn bench_bincode_message_serialize(b: &mut Bencher) {
    let message = make_message();
    b.iter(|| {
        test::black_box(serialize(&message).unwrap());
    });
}

#[bench]
fn bench_manual_message_serialize(b: &mut Bencher) {
    let message = make_message();
    b.iter(|| {
        test::black_box(message.manual_serialize());
    });
}

#[bench]
fn bench_bincode_message_deserialize(b: &mut Bencher) {
    let message = make_message();
    let serialized = message.serialize();
    b.iter(|| {
        test::black_box(deserialize::<Message>(&serialized).unwrap());
    });
}

#[bench]
fn bench_manual_message_deserialize(b: &mut Bencher) {
    let message = make_message();
    let serialized = message.manual_serialize();
    b.iter(|| {
        test::black_box(Message::from_bytes(&serialized).unwrap());
    });
}
