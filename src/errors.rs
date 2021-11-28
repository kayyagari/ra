use thiserror::Error;

#[derive(Debug, Error)]
pub enum RaError {
    #[error("{0}")]
    DbError(String),
}

#[cfg(test)]
mod tests {
    use crate::errors::{RaError};

    #[test]
    fn test_error() {
        let re = RaError::DbError(String::from("this is the message"));
        println!("{:?}", re);
    }
}
