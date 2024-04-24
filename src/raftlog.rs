#[derive(Debug)]
pub struct Entry<T> {
    command: T,
    term: usize,
}

#[derive(Debug)]
pub struct Log<T>(Vec<Entry<T>>);

impl<T> Log<T> {
    pub fn new() -> Self {
        Self(vec![])
    }
}
