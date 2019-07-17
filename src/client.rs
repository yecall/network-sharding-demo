use crossbeam_channel::{self as channel, Receiver, Sender, TryRecvError};
use std::{thread, cmp, time::Duration};
use futures::{prelude::*, future, stream, Stream, stream::Fuse};
use libp2p::tokio_codec::{Framed, FramedRead, LinesCodec};
use log::info;
use std::sync::Arc;
use futures::task::AtomicTask;
use regex::Regex;

#[derive(Debug)]
pub enum Message {
    Transaction {
        from: u16,
        to: u16,
    },
    Block {
        shard_num: u16,
        height: u64,
    },
    BlockHead {
        shard_num: u16,
        height: u64,
    },
}

pub trait ParseMessage {
    fn parse_message(&self) -> Option<Message>;
}

impl ParseMessage for String {
    fn parse_message(&self) -> Option<Message> {
        let re = Regex::new(r"Transaction\(\s*from\s*:\s*(\d+)\s*,\s*to\s*:\s*(\d+)\s*\)").unwrap();
        let m: Vec<(u16, u16)> = re.captures_iter(&self).map(|c| (c.get(1).unwrap().as_str().parse().unwrap(), c.get(2).unwrap().as_str().parse().unwrap())).collect();
        if m.len() > 0 {
            let (from, to) = m.get(0).unwrap();
            return Some(Message::Transaction { from: from.to_owned(), to: to.to_owned() });
        }

        let re = Regex::new(r"Block\(\s*shard_num\s*:\s*(\d+)\s*,\s*height\s*:\s*(\d+)\s*\)").unwrap();
        let m: Vec<(u16, u64)> = re.captures_iter(&self).map(|c| (c.get(1).unwrap().as_str().parse().unwrap(), c.get(2).unwrap().as_str().parse().unwrap())).collect();
        if m.len() > 0 {
            let (shard_num, height) = m.get(0).unwrap();
            return Some(Message::Block { shard_num: shard_num.to_owned(), height: height.to_owned() });
        }

        let re = Regex::new(r"BlockHead\(\s*shard_num\s*:\s*(\d+)\s*,\s*height\s*:\s*(\d+)\s*\)").unwrap();
        let m: Vec<(u16, u64)> = re.captures_iter(&self).map(|c| (c.get(1).unwrap().as_str().parse().unwrap(), c.get(2).unwrap().as_str().parse().unwrap())).collect();
        if m.len() > 0 {
            let (shard_num, height) = m.get(0).unwrap();
            return Some(Message::BlockHead { shard_num: shard_num.to_owned(), height: height.to_owned() });
        }
        None
    }
}

pub struct Client {
    pub stdin_sender: Sender<String>,
    pub stdin_receiver: Receiver<String>,
    pub stdin_notify: Arc<AtomicTask>,
}

impl Client {
    pub fn new() -> Self {
        let (stdin_sender, stdin_receiver) = channel::unbounded();
        ;

        let stdin_notify = Arc::new(AtomicTask::new());
        ;

        Client {
            stdin_sender,
            stdin_receiver,
            stdin_notify,
        }
    }

    pub fn run(&self) {
        let stdin = tokio_stdin_stdout::stdin(0);
        let mut framed_stdin = FramedRead::new(stdin, LinesCodec::new());

        let sender = self.stdin_sender.clone();
        let notify = self.stdin_notify.clone();

        let thread = thread::Builder::new().name("client".to_string()).spawn(move || {
            tokio::run(future::poll_fn(move || -> Result<_, ()> {
                loop {
                    match framed_stdin.poll().expect("Error while polling stdin") {
                        Async::Ready(Some(line)) => {
                            info!("Stdin: {}", line);
                            let result = sender.send(line);
                            notify.notify();
                            result
                        }
                        Async::Ready(None) => panic!("Stdin closed"),
                        Async::NotReady => {
                            return Ok(Async::NotReady);
                        }
                    };
                }
            }));
        });

        info!("Run client successfully");
    }
}