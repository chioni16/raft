use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt::Debug;

use crate::rpc;

pub trait Command: Debug + Default + Clone + Send + Serialize + DeserializeOwned + 'static {}

impl<T> Command for T where
    T: Debug + Default + Clone + Send + Serialize + DeserializeOwned + 'static
{
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(bound = "T: Command")]
pub struct Entry<T> {
    pub command: T,
    pub term: u64,
}

impl<T: Command> Entry<T> {
    pub fn new(command: T, term: u64) -> Self {
        Self { command, term }
    }
}

impl<T: Command> From<rpc::LogEntry> for Entry<T> {
    fn from(value: rpc::LogEntry) -> Self {
        Self {
            command: serde_json::from_str::<T>(&value.command).unwrap(),
            term: value.term,
        }
    }
}

impl<T: Command> Into<rpc::LogEntry> for Entry<T> {
    fn into(self) -> rpc::LogEntry {
        rpc::LogEntry {
            command: serde_json::to_string(&self.command).unwrap(),
            term: self.term,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(bound = "T: Command")]
pub struct Log<T>(Vec<Entry<T>>);

impl<T: Command> Log<T> {
    pub fn new() -> Self {
        Self(vec![Default::default()])
    }

    pub fn len(&self) -> u64 {
        self.0.len() as u64
    }

    pub fn truncate(&mut self, index: u64) {
        self.0.truncate(index as usize);
    }

    pub fn extend(&mut self, v: Vec<Entry<T>>) {
        self.0.extend(v);
    }

    pub fn get_last_log_index(&self) -> u64 {
        self.0.len() as u64
    }

    pub fn get_last_log_term(&self) -> u64 {
        self.0.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn get_log_term(&self, index: u64) -> u64 {
        self.0.get(index as usize).map(|e| e.term).unwrap()
    }

    pub fn add_new_entry(&mut self, entry: Entry<T>) {
        self.0.push(entry);
    }

    pub fn get_entries_from(&self, index: u64) -> Vec<Entry<T>> {
        self.0[index as usize..].to_vec()
    }

    pub fn get_entries_in_range(&self, inclusive_start: u64, exclusive_end: u64) -> Vec<Entry<T>> {
        self.0[inclusive_start as usize..exclusive_end as usize].to_vec()
    }
}

// use std::ops::{Deref, DerefMut};

// impl<T> Deref for Log<T> {
//     type Target = Vec<Entry<T>>;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

// impl<T> DerefMut for Log<T> {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.0
//     }
// }
