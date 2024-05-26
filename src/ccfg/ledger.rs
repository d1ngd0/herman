use std::{
    collections::HashMap,
    hash::Hash,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

// Key is a trait that we use to define the key type for the Ledger.
pub trait Key: Eq + Clone + Hash + Send + Sync + 'static {}
impl<T: Eq + Clone + Hash + Send + Sync + 'static> Key for T {}

pub trait Data: Clone + Send + Sync + 'static {}
impl<T: Clone + Send + Sync + 'static> Data for T {}

pub struct Ledger<T: Key, D: Data> {
    entries: Vec<Entry<T, D>>,
}

impl<T: Key, D: Data> Ledger<T, D> {
    // new creates a new cluster config
    pub fn new() -> Ledger<T, D> {
        Ledger {
            entries: Vec::new(),
        }
    }

    // put will add a new entry to the config and
    // returns a pointer to the entry.
    pub fn put(&mut self, key: T, value: D) -> &Entry<T, D> {
        self.entries.push(Entry::Put(
            EntryMeta {
                key: key.clone(),
                at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            },
            value,
        ));
        self.entries.last().unwrap()
    }

    pub fn delete(&mut self, key: T) -> &Entry<T, D> {
        self.entries.push(Entry::Delete(EntryMeta {
            key: key.clone(),
            at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        }));
        self.entries.last().unwrap()
    }

    // add_entry adds the entry to the config. If the entry is already present
    // it will not be added, and we return false. Otherwise, we add it and
    // return true.
    pub fn add_entry(&mut self, entry: Entry<T, D>) -> bool {
        for e in self.entries.iter() {
            if e == &entry {
                return false;
            }
        }

        self.entries.push(entry);
        true
    }

    // get returns the matching key. It parses the whole config, making sure we
    // grab the latest value or respect the most recent delete.
    pub fn get<ET: PartialEq<T>>(&self, key: ET) -> Option<&D> {
        let mut val = None;
        for entry in self.entries.iter() {
            match entry {
                Entry::Put(meta, value) => {
                    if key == meta.key {
                        val = Some(value);
                    }
                }
                Entry::Delete(meta) => {
                    if key == meta.key {
                        val = None;
                    }
                }
            }
        }

        val
    }

    pub fn sort(&mut self) {
        self.entries.sort_by(|a, b| match (a, b) {
            (Entry::Put(a, _), Entry::Put(b, _)) => a.at.cmp(&b.at),
            (Entry::Delete(a), Entry::Delete(b)) => a.at.cmp(&b.at),
            (Entry::Put(a, _), Entry::Delete(b)) => a.at.cmp(&b.at),
            (Entry::Delete(a), Entry::Put(b, _)) => a.at.cmp(&b.at),
        });
    }

    pub fn compact(&mut self, upto: Duration) {
        // fetch the timestamp
        let upto = SystemTime::now()
            .checked_sub(upto)
            .expect("Invalid duration")
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        // grab everything that needs to be removed
        let remove: Vec<T> = self
            .entries
            .iter()
            .filter_map(|entry| match entry {
                Entry::Delete(meta) => {
                    if meta.at < upto {
                        Some(meta.key.clone())
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();

        self.entries.retain(|entry| {
            let meta = match entry {
                Entry::Put(meta, _) => meta,
                Entry::Delete(meta) => meta,
            };

            if meta.at < upto {
                !remove.contains(&meta.key)
            } else {
                true
            }
        });
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Entry<T: Key, D: Data> {
    Put(EntryMeta<T>, D),
    Delete(EntryMeta<T>),
}

impl<T: Key, D: Data> PartialEq for Entry<T, D> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Entry::Put(a, _), Entry::Put(b, _)) => a == b,
            (Entry::Delete(a), Entry::Delete(b)) => a == b,
            _ => false,
        }
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct EntryMeta<T: Key> {
    key: T,
    at: u128,
}

impl<T: Key, D: Data> From<Ledger<T, D>> for HashMap<T, D> {
    fn from(ccfg: Ledger<T, D>) -> HashMap<T, D> {
        let mut map = HashMap::new();

        for entry in ccfg.entries {
            match entry {
                Entry::Put(meta, value) => {
                    map.insert(meta.key, value);
                }
                Entry::Delete(meta) => {
                    map.remove(&meta.key);
                }
            }
        }

        map
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_cluster_config() {
        let mut ccfg = Ledger::new();
        ccfg.put("key1", "value1");
        ccfg.put("key2", "value2");
        ccfg.put("key3", "value3");

        assert_eq!(ccfg.get("key1"), Some(&"value1"));
        assert_eq!(ccfg.get("key2"), Some(&"value2"));
        assert_eq!(ccfg.get("key3"), Some(&"value3"));

        ccfg.delete("key2");
        assert_eq!(ccfg.get("key2"), None);

        ccfg.compact(Duration::from_secs(1));
        assert_eq!(ccfg.get("key1"), Some(&"value1"));
        assert_eq!(ccfg.get("key3"), Some(&"value3"));

        let settings: HashMap<&str, &str> = ccfg.into();
        assert_eq!(settings.len(), 2);
        assert_eq!(settings.get("key1"), Some(&"value1"));
        assert_eq!(settings.get("key3"), Some(&"value3"));
    }

    #[test]
    fn test_add_entry() {
        let mut ccfg = Ledger::new();
        let entry = Entry::Put(EntryMeta { key: "key1", at: 0 }, "value1");

        assert!(ccfg.add_entry(entry.clone()));
        assert!(!ccfg.add_entry(entry.clone()));
    }
}
