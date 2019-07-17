use crossbeam_channel::{self as channel, Receiver, Sender, TryRecvError};
use std::{thread, cmp, time::Duration};
use futures::{prelude::*, future, stream, Stream, stream::Fuse};
use libp2p::tokio_codec::{Framed, FramedRead, LinesCodec};
use log::info;
use std::sync::Arc;
use futures::task::AtomicTask;

pub struct Client {
    pub stdin_sender: Sender<String>,
    pub stdin_receiver: Receiver<String>,
    pub stdin_notify: Arc<AtomicTask>,
}

impl Client {
    pub fn new() -> Self {
        let (stdin_sender, stdin_receiver) = channel::unbounded();;

        let stdin_notify = Arc::new(AtomicTask::new());;

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
                        },
                        Async::Ready(None) => panic!("Stdin closed"),
                        Async::NotReady => {
                            return Ok(Async::NotReady)
                        },
                    };
                }
            }));
        });

        info!("Run client successfully");
    }
}