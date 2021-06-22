#[macro_use]
extern crate abomonation_derive;

pub mod vsknn;
pub mod differential;
pub mod io;
pub mod stopwatch;

fn linear_score(pos: usize) -> f64 {
    if pos < 100 { 1.0 - (0.1 * pos as f64) } else { 0.0 }
}

