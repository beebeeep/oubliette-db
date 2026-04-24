use sexpression::Expression as Sexpr;

use crate::error::{self, AppError};

pub(crate) fn assert_longer(v: &[Sexpr], len: usize) -> Result<(), AppError> {
    if v.len() <= len {
        error::BadRequest {
            e: format!("invalid length of expression {v:?}: more than {len} expected"),
        }
        .fail()?
    }
    Ok(())
}

pub(crate) fn assert_len(v: &[Sexpr], len: usize) -> Result<(), AppError> {
    if v.len() != len {
        error::BadRequest {
            e: format!("invalid length of expression {v:?}: {len} expected"),
        }
        .fail()?
    }
    Ok(())
}
