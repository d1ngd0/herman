use std::{collections::HashMap, sync::Arc};

use arrayvec::ArrayVec;
use dapt::Dapt;
use tokio::{
    sync::{
        mpsc::{channel, error::TryRecvError, Receiver, Sender},
        Mutex,
    },
    task,
};

const MAX_REMOVE: usize = 8;

// Router is a collection of namespaces which allows for registering
// channels to read and write to namespace. Namespaces are created
// as needed in the router during registration. If the all input and
// output registers have been closed the namespace will automatically
// be cleaned up.
pub struct Router {
    namespaces: Mutex<HashMap<String, Namespace>>,
}

impl Router {
    pub fn new() -> Self {
        Router {
            namespaces: Mutex::new(HashMap::new()),
        }
    }

    pub async fn namespace(&mut self, name: &str) -> NamespaceRegister {
        let mut namespaces = self.namespaces.lock().await;

        let namespace = namespaces
            .entry(name.to_string())
            .or_insert_with(Namespace::new);

        let mut runner = namespace.runner();
        tokio::spawn(async move {
            runner.start().await;
        });

        namespace.register()
    }
}

#[derive(Clone)]
pub struct NamespaceRegister {
    inputs: Inputs,
    outputs: Outputs,
    done: ClosedNamespace,
}

impl NamespaceRegister {
    pub async fn input(&mut self, buffer: usize) -> Sender<Dapt> {
        let (send, recv) = channel(buffer);
        self.inputs.lock().await.push(recv);
        send
    }

    pub async fn output(&mut self, buffer: usize) -> Receiver<Dapt> {
        let (send, recv) = channel(buffer);
        self.outputs.lock().await.push(send);
        recv
    }
}

// convience types
type Inputs = Arc<Mutex<Vec<Receiver<Dapt>>>>;
type Outputs = Arc<Mutex<Vec<Sender<Dapt>>>>;
type ClosedNamespace = Arc<Mutex<bool>>;

// Namespace is a location to place dapt packets. When registering to a
// namespace to write a channel is created to send dapt packets. When
// reading data from a namespace, a channel is provided to read dapt
// packets. Each output registered will recieve all of the data in the
// namespace. The namespace itself does not buffer any data, though buffers
// can be set on the registers themselves.
pub struct Namespace {
    inputs: Inputs,
    outputs: Outputs,
    done: ClosedNamespace,
}

impl Namespace {
    pub fn new() -> Self {
        let inputs = Arc::new(Mutex::new(Vec::new()));
        let outputs = Arc::new(Mutex::new(Vec::new()));

        Namespace {
            inputs,
            outputs,
            done: Arc::new(Mutex::new(false)),
        }
    }

    // runner creates
    pub fn runner(&self) -> NamespaceRunner {
        NamespaceRunner {
            inputs: self.inputs.clone(),
            outputs: self.outputs.clone(),
            done: self.done.clone(),
        }
    }

    pub fn register(&mut self) -> NamespaceRegister {
        NamespaceRegister {
            inputs: self.inputs.clone(),
            outputs: self.outputs.clone(),
            done: self.done.clone(),
        }
    }
}

impl Default for Namespace {
    fn default() -> Self {
        Namespace::new()
    }
}

// when rust allows us to do this we will. For now, when we remove
// a namespace from the map, we need to set the done flag to true
// so things clean themselves up.
// impl AsyncDrop for Namespace {
//     async fn drop(&mut self) {
//         // set to done so the namespace future returns ready
//         *self.done.lock().await = true;
//     }
// }

pub struct NamespaceRunner {
    inputs: Inputs,
    outputs: Outputs,
    done: ClosedNamespace,
}

impl NamespaceRunner {
    pub async fn start(&mut self) {
        loop {
            // get out inputs and outputs, if we can't get them because we
            // are currently adding registers then return pending
            let mut inputs = self.inputs.lock().await;
            let mut outputs = self.outputs.lock().await;

            // use ArrayVec to avoid a heap allocation
            let mut remove_inputs: ArrayVec<usize, MAX_REMOVE> = ArrayVec::new();
            let mut remove_outputs: ArrayVec<usize, MAX_REMOVE> = ArrayVec::new();

            for (i, input) in inputs.iter_mut().enumerate() {
                let p = input.try_recv();
                match p {
                    Err(TryRecvError::Disconnected) => {
                        // we just want to remove up to MAX_REMOVE at a time
                        // so if we can't add to the remove list we will get
                        // it next time
                        let _ = remove_inputs.try_push(i);
                    }
                    Err(TryRecvError::Empty) => {
                        task::yield_now().await; // give execution back to the scheduler
                    } // just move on
                    Ok(d) => {
                        for output in outputs.iter_mut() {
                            // if the downstream channel has been deleted we will add this to
                            // the remove list
                            if let Err(_) = output.send(d.clone()).await {
                                let _ = remove_outputs.try_push(i);
                            }
                        }
                    }
                }
            }

            remove_inputs.iter().for_each(|i| {
                inputs.swap_remove(*i);
            });

            remove_outputs.iter().for_each(|i| {
                outputs.swap_remove(*i);
            });

            if *self.done.lock().await {
                break;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use dapt::*;

    #[tokio::test]
    // This tests the namespaces functionality across threads
    async fn test_namespace() {
        let mut ns = Namespace::new();
        let mut reg = ns.register();

        let mut runner = ns.runner();
        task::spawn(async move {
            runner.start().await;
        });

        // create two threads which consume all the data
        let mut output = reg.output(10).await;
        let thread1 = task::spawn(async move {
            for i in 0..100 {
                let o = output.recv().await.unwrap();
                assert_eq!(o.number("test").unwrap(), i);
            }
        });

        let mut output = reg.output(10).await;
        let thread2 = task::spawn(async move {
            for i in 0..100 {
                let o = output.recv().await.unwrap();
                assert_eq!(o.number("test").unwrap(), i);
            }
        });

        // write the input
        let input = reg.input(10).await;
        for i in 0..100 {
            let mut d = DaptBuilder::new();
            d.set("test", i).unwrap();
            input.send(d.build()).await.unwrap();
        }

        // make sure both threads run to completion
        thread1.await.unwrap();
        thread2.await.unwrap();
    }

    #[tokio::test]
    // This tests the namespaces functionality across threads
    async fn test_router() {
        let mut router = Router::new();
        let mut test = router.namespace("test").await;

        // create two threads which consume all the data
        let mut output = test.output(10).await;
        let thread1 = task::spawn(async move {
            for i in 0..100 {
                let o = output.recv().await.unwrap();
                assert_eq!(o.number("test").unwrap(), i);
            }
        });

        let mut output = test.output(10).await;
        let thread2 = task::spawn(async move {
            for i in 0..100 {
                let o = output.recv().await.unwrap();
                assert_eq!(o.number("test").unwrap(), i);
            }
        });

        // write the input
        let input = test.input(10).await;
        for i in 0..100 {
            let mut d = DaptBuilder::new();
            d.set("test", i).unwrap();
            input.send(d.build()).await.unwrap();
        }

        // make sure both threads run to completion
        thread1.await.unwrap();
        thread2.await.unwrap();
    }
}
