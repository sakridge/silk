/// network is arranged like a heap
///         1 loom
/// M highest staked nodes
/// N lower staked nodes
/// O even lower staked nodes
/// where O < M < N in stake
///   1
/// M 10
/// N 100
/// O 1000
/// each of the N nodes gets 1/10th of its bandwidth from any single M node
/// 10x can be configurable by the network, see FANOUT
///
/// nodes on each level maintain p2p connections to receive the rest of the traffic
/// 3 services
/// * broadcast, sends data downstream
/// * receiver, gets upstream data, and fetches missing packets from peers and parents
/// * peer_server, that accepts requests from peers and children data

use signature::PublicKey;
use bincode::{deserialize};
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;
use std::sync::{Arc, Mutex};
use rand::{Rng, thread_rng};
use std::sync::RwLock;
use std::sync::mpsc::{Receiver, Sender};
use packet::PacketData;
use result::Result;

/// use A/100 amount of bandwidth to reed solomon code the output
const CODES: u64 = 256;
/// how many more nodes in each layer
const FANOUT: u64 = 1024;
const WINDOW: u64 = 1024*1024;

type Amount = i64;
type SubscriptionHeap = Arc<RwLock<Vec<Subscription>>>;

struct Subscription {
    key: PublicKey, 
    stake: Amount,
    addr: SocketAddr,
}

pub type SharedPacketData = Arc<RwLock<PacketData>>;
pub type Cache = Arc<Mutex<Vec<SharedPacketData>>>;

struct Subscriptions {
    recycler: Cache,
    window: Cache,
    heap: SubscriptionHeap,
    me: PublicKey,
}

fn heap_update(subs: SubscriptionHeap, sub: Subscription) {
}

fn is_child(subs: SubscriptionHeap, pubkey: PublicKey) -> bool {
    true
}

fn is_peer(subs: SubscriptionHeap, pubkey: PublicKey) -> bool {
    true
}

fn get_peers(subs: SubscriptionHeap, pubkey: PublicKey) -> Vec<Subscription> {
    vec![]
}

fn get_children(subs: SubscriptionHeap, pubkey: PublicKey) -> Vec<Subscription> {
    vec![]
}

fn get_parents(subs: SubscriptionHeap, pubkey: PublicKey) -> Vec<Subscription> {
    vec![]
}

fn get_index(subs: SubscriptionHeap, pubkey: PublicKey) -> usize {
    return 0;
}

impl Subscriptions {
    fn peers(&self) -> Vec<Subscription> {
        get_peers(self.heap, self.me)
    }

    fn parents(&self) -> Vec<Subscription> {
        get_parents(self.heap, self.me)
    }

    fn children(&self) -> Vec<Subscription> {
        get_children(self.heap, self.me)
    }

    fn my_peer_index(&self) -> usize {
        get_index(self.heap, self.me)
    }

    /// Assume all the peers are sending to all the children
    /// Broadcast to as many children as you can, avoid sending duplicates
    pub fn broadcast(&self, sock: UdpSocket, r: Receiver<SharedPacketData>) -> Result<()> {
        let packets = r.recv_timeout(Duration::new(1, 0))?;
        self.window.write().push(packets);
        let peers = self.peers();
        let me = self.my_peer_index();
        let children = self.children();
        for p in packets.iter() {
            p.send_to(sock, children[(p.index() + me) % children.len()]);
        }
        Ok(())
    }

    /// Wait for a block of packets to be filled and then send it through the channel.
    /// Check the min number necessary to reconstruct the data set
    /// randomly ask a peer or parent for the missing packets in random order
    pub fn receiver(&self, r: Receiver<SharedPacketData>, s: Sender<SharedPacketData>)-> Result<()> {
        let mut packets = self.window_peek();
        //let required = (* 100) / FANOUT;
        loop {
            packets.fill_packets(s)?;
            let t = (FANOUT - CODES) * packets.len();
            let min_needed = (t + (FANOUT / 2)) / FANOUT;
            if packets.num_valid() <= min_needed {
                break;
            }
            let mut num = packets.num_valid() - min_needed;
            let c = self.peers().extend(self.parents());
            let mut rng = thread_rng();
            for ix in rng.shuffle(0..packets.len()) {
                if num == 0 {
                    break;
                }
                let p = packets[ix];
                if p.valid() {
                    continue;
                }
                let r = c[rng.gen_range(0, c.len())];
                s.send_to(r, p.request());
                num -= 1;
            }
        }
        let done = self.window_pop()?;
        s.send(done);
        Ok(())
    }
    /// Assume all the peers are sending to all the children
    /// Broadcast to as many children as you can, avoid sending duplicates
    /// p is parent, c is child
    /// p1, p2
    /// c1, c2, c3, c4
    /// p1 sends packet: 1,5; 2,6;      3;      4
    /// p2 sends packet: 4;   1,5;    2,6;      3
    /// c1 sends:        _;     _;    1,5;    1,5
    /// c2 sends:      2,6;     _;      _;    2,6
    /// c3 sends:        3;     3;      _;      _
    /// c4 sends:        _;     4;      4;      _  
    pub fn rebroadcast(&self, sock: UdpSocket) -> Result<()> {
        let packets = self.window_peak()?;
        let me = self.my_peer_index();
        let peers = self.peers();
        let parents_len = self.parents().len();
        for p in packets.iter_mut() {
            if p.transmitted {
                continue;
            }
            if !p.valid {
                continue;
            }
            if p.index() % peers.len() == me {
                let end = (me + parents_len) % peers.len();
                if end < me {
                    p.send_to(peers[end .. me])?;
                } else {
                    p.send_to(peers[0 .. me])?;
                    p.send_to(peers[end .. ])?;
                }
            }
            p.transmitted = true;
        }
        Ok(())
    }

    /// Wait for requests form peers, then respond to them
    pub fn peer_server(&self, s: UdpSocket, requests: Receiver<SharedPacketData>) -> Result<()> {
        requests.recv()?;
        for r in requests.iter() {
            let req = deserialize(r.data[0..r.len()]);
            let packet = self.window_lookup(req);
            packet.send_to(r.get_addr());
        }
        Ok(())
    }
}
