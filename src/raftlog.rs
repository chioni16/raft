use crate::rpc;

#[derive(Debug, Clone, Default)]
pub struct Entry<T: Clone> {
    pub command: T,
    pub term: u64,
}

impl<T: Clone> Entry<T> {
    pub fn new(command: T, term: u64) -> Self {
        Self { command, term }
    }
}

impl<T: Clone + From<String>> From<rpc::LogEntry> for Entry<T> {
    fn from(value: rpc::LogEntry) -> Self {
        Self {
            command: value.command.into(),
            term: value.term,
        }
    }
}

impl<T: Clone + Into<String>> Into<rpc::LogEntry> for Entry<T> {
    fn into(self) -> rpc::LogEntry {
        rpc::LogEntry {
            command: self.command.into(),
            term: self.term,
        }
    }
}

#[derive(Debug)]
pub struct Log<T: Clone>(Vec<Entry<T>>);

impl<T: Default + Clone> Log<T> {
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
