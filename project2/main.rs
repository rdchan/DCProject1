extern crate bincode;
extern crate serde;

use std::collections::{HashMap, HashSet};
use std::env;
use std::io::{Read, Write};
use std::net::Shutdown;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread::sleep;
use std::time::Duration;

use serde::{Deserialize, Serialize};

const MAX_MESSAGE_SIZE: usize = 4096;

#[derive(Debug, Clone)]
struct Node {
    ip: String,
    port: u16,
    neighbors: Vec<i32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum MessageType {
    Sync,
    Stop,
    Done,
    Peleg(i32, i32, bool), // highest_uid, longest_distance, is_child
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Message {
    msg_type: MessageType,
    round: i32,
    sender: i32,
    receiver: i32,
}

fn send_message(msg: Message, stream: &mut TcpStream) {
    let bytes: Vec<u8> = msg.into();
    stream.write(&bytes).expect("Unable to write to stream");
}

fn recv_message(stream: &mut TcpStream) -> Option<Message> {
    let mut buf = vec![0; MAX_MESSAGE_SIZE];
    let n = stream.read(&mut buf).expect("Unable to read from stream");
    if n == 0 {
        return None;
    }
    let msg: Message = buf[..n].into();
    Some(msg)
}

impl From<&[u8]> for Message {
    fn from(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("Unable to deserialize message")
    }
}

impl From<Message> for Vec<u8> {
    fn from(msg: Message) -> Self {
        bincode::serialize(&msg).expect("Unable to serialize message")
    }
}

fn read_config(file_name: &str) -> HashMap<i32, Node> {
    let mut nodes = HashMap::new();
    let file_str = std::fs::read_to_string(file_name).expect("Unable to read file");

    let mut lines = file_str
        .lines()
        .map(|line| line.trim())
        .filter(|line| line.starts_with(|c: char| c.is_digit(10)));

    let num_nodes;
    if let Some(line) = lines.next() {
        num_nodes = line
            .parse::<i32>()
            .expect("Unable to parse number of nodes");
    } else {
        panic!("Unable to read number of nodes");
    }

    let mut node_lines = lines.clone().take(num_nodes as usize);
    let mut neighbor_lines = lines.skip(num_nodes as usize);

    for _ in 0..num_nodes {
        if let Some(line) = node_lines.next() {
            let mut parts = line.split_whitespace();
            let id = parts
                .next()
                .expect("No node id found")
                .parse::<i32>()
                .expect("Unable to parse node id");
            let ip = parts.next().expect("No node ip found").to_string();
            let port = parts
                .next()
                .expect("No node port found")
                .parse::<u16>()
                .expect("Unable to parse node port");

            let mut neighbors = Vec::new();
            if let Some(line) = neighbor_lines.next() {
                let parts = line.split_whitespace();
                for part in parts {
                    let neighbor_id = part.parse::<i32>().expect("Unable to parse neighbor id");
                    neighbors.push(neighbor_id);
                }
            }
            nodes.insert(
                id,
                Node {
                    ip,
                    port,
                    neighbors,
                },
            );
        }
    }

    return nodes;
}

fn connect_to_neighbors(node_id: i32, nodes: &HashMap<i32, Node>) -> HashMap<i32, TcpStream> {
    let node = nodes.get(&node_id).expect("Unable to find node id");
    let listener =
        TcpListener::bind(format!("{}:{}", node.ip, node.port)).expect("Unable to bind to port");
    let mut listeners = HashMap::new();
    for neighbor in &node.neighbors {
        let socket: TcpStream;
        let addr: SocketAddr;
        if node_id < *neighbor {
            loop {
                match TcpStream::connect(format!("{}:{}", nodes[neighbor].ip, nodes[neighbor].port))
                {
                    Ok(s) => {
                        socket = s;
                        addr = socket.peer_addr().expect("Unable to get peer address");
                        break;
                    }
                    Err(_) => {
                        println!("Unable to connect to {}, retrying...", neighbor);
                        sleep(Duration::from_secs(1));
                    }
                }
            }
        } else {
            // accept connections from neighbors with lower id
            (socket, addr) = listener.accept().expect("Unable to accept connection");
        }
        println!("Connection established with {}", addr);

        listeners.insert(*neighbor, socket);
    }
    return listeners;
}

fn run_peleg(node_id: i32, nodes: &HashMap<i32, Node>) -> (i32, HashSet<i32>) {
    let mut streams = connect_to_neighbors(node_id, &nodes);

    let mut rounds_since_update = 0;
    let mut my_longest_distance = 0;
    let mut my_highest_uid = node_id;
    let mut parent = node_id;

    let mut stop = -1;
    let mut done = HashSet::new();
    let mut kids = HashSet::new();
    let mut round = 0;

    // Send initial peleg message to all neighbors
    for (id, stream) in &mut streams {
        send_message(
            Message {
                msg_type: MessageType::Peleg(node_id, 0, false), //  <- highest_uid: i32, longest_distance: i32, is_child: bool
                round: 0,
                sender: node_id,
                receiver: *id,
            },
            stream,
        );
    }

    loop {
        rounds_since_update += 1;
        println!("Round {}", round);

        if rounds_since_update > 3 {
            stop = node_id;
        }

        // Receive messages from neighbors
        for (id, stream) in &mut streams {
            if done.contains(id) {
                continue;
            }

            if let Some(msg) = recv_message(stream) {
                if msg.round > round + 1 {
                    panic!("Received sync message from {} with round number {} but current round is {}", id, msg.round, round);
                }
                match msg.msg_type {
                    MessageType::Sync => {}
                    MessageType::Peleg(highest_uid, longest_distance, is_child) => {
                        if highest_uid > my_highest_uid {
                            my_highest_uid = highest_uid;
                            my_longest_distance = longest_distance + 1;
                            parent = *id;
                            rounds_since_update = 0;
                        } else if highest_uid == my_highest_uid {
                            if longest_distance > my_longest_distance {
                                my_longest_distance = longest_distance;
                                rounds_since_update = 0;
                            }
                        }

                        if is_child && !kids.contains(id) {
                            kids.insert(*id);
                        } else if !is_child && kids.contains(id) {
                            kids.remove(id);
                        }
                    }
                    MessageType::Stop => {
                        stop = *id;
                        rounds_since_update = 0;
                    }
                    MessageType::Done => {
                        done.insert(*id);
                        stream
                            .shutdown(Shutdown::Both)
                            .expect("Unable to shutdown socket");
                        continue;
                    }
                }
            }

            let msg_type;
            if stop == -1 && rounds_since_update <= 1 {
                msg_type = MessageType::Peleg(my_highest_uid, my_longest_distance, *id == parent); // <- highest_uid, longest_distance, is_child
            } else if stop != *id && stop != -1 {
                msg_type = MessageType::Stop;
            } else if stop == *id {
                done.insert(*id);
                msg_type = MessageType::Done;
            } else {
                msg_type = MessageType::Sync;
            }

            let msg = Message {
                msg_type,
                round,
                sender: node_id,
                receiver: *id,
            };
            send_message(msg, stream);
        }

        round += 1;

        if done.len() == streams.len() {
            break;
        }
    }

    return (parent, kids);
}

fn main() {
    let node_id = env::args()
        .nth(1)
        .expect("No node id provided")
        .parse::<i32>()
        .expect("Unable to parse node id");
    let file_name = env::args().nth(2).expect("No config file provided");
    let nodes = read_config(&file_name);

    for (id, node) in &nodes {
        println!("Node {}: {:?}", id, node);
    }

    let (parent, kids) = run_peleg(node_id, &nodes);
    println!("I am node {}", node_id);
    println!("My parent is {}", parent);
    println!("My children are {:?}", kids);
    println!("My degree is {}", kids.len() + 1);
}
