#![feature(test)]

extern crate test;

use rand::{thread_rng, Rng};
use solana_core::broadcast_stage::broadcast_metrics::TransmitShredsStats;
use solana_core::broadcast_stage::{broadcast_shreds, get_broadcast_peers};
use solana_core::cluster_info::{ClusterInfo, Node};
use solana_core::contact_info::ContactInfo;
use solana_ledger::shred::Shred;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::timing::timestamp;
use std::{
    collections::HashMap,
    net::UdpSocket,
    sync::{atomic::AtomicU64, Arc},
};
use test::Bencher;

#[bench]
fn broadcast_shreds_bench(bencher: &mut Bencher) {
    solana_logger::setup();
    let leader_pubkey = Pubkey::new_rand();
    let leader_info = Node::new_localhost_with_pubkey(&leader_pubkey);
    let cluster_info = ClusterInfo::new_with_invalid_keypair(leader_info.info);
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();

    const NUM_SHREDS: usize = 32;
    let shreds = vec![Shred::new_empty_data_shred(); NUM_SHREDS];
    let mut stakes = HashMap::new();
    const NUM_PEERS: usize = 200;
    for _ in 0..NUM_PEERS {
        let id = Pubkey::new_rand();
        let contact_info = ContactInfo::new_localhost(&id, timestamp());
        cluster_info.insert_info(contact_info);
        stakes.insert(id, thread_rng().gen_range(1, NUM_PEERS) as u64);
    }
    let stakes = Arc::new(stakes);
    let cluster_info = Arc::new(cluster_info);
    let (peers, peers_and_stakes) = get_broadcast_peers(&cluster_info, Some(stakes));
    let shreds = Arc::new(shreds);
    let last_datapoint = Arc::new(AtomicU64::new(0));
    bencher.iter(move || {
        let shreds = shreds.clone();
        broadcast_shreds(
            &socket,
            &shreds,
            &peers_and_stakes,
            &peers,
            &last_datapoint,
            &mut TransmitShredsStats::default(),
        )
        .unwrap();
    });
}


#[bench]
fn bench_pull_response(bencher: &mut Bencher) {
    solana_logger::setup();
    use solana_core::cluster_info::PullData;
    use solana_core::crds_gossip_pull::CrdsFilter;
    use solana_core::crds_value::{CrdsValue, CrdsData};
    use solana_perf::packet::PacketsRecycler;
    use solana_sdk::signature::Signature;
    let cluster_info = ClusterInfo::new_with_invalid_keypair(ContactInfo::default());
    for _ in 0..100_000 {
        cluster_info.push_lowest_slot(Pubkey::new_rand(), thread_rng().gen_range(0, 100));
    }
    let recycler = PacketsRecycler::default();
    let pull_requests: Vec<_> = (0..100).into_iter().map(|_| {
        let crds_data = CrdsData::ContactInfo(ContactInfo::default());
        let caller = CrdsValue { signature: Signature::default(), data: crds_data };
        let filter = CrdsFilter::default();
        let from_addr = "127.0.0.1:800".parse().unwrap();
        PullData { from_addr, caller, filter }
    }).collect();
    let stakes = HashMap::new();
    bencher.iter(move || {
        let _ = cluster_info.handle_pull_requests(&recycler, pull_requests.clone(), &stakes);
    });
}
