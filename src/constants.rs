use math::round;

pub const NUM_SERVERS: usize = 3;
pub const MIN_TIMEOUT: usize = 150;
pub const MAX_TIMEOUT: usize = 300;
pub const MESSAGE_LENGTH: usize = 1024;

lazy_static! {
    pub static ref MIN_QUORUM: usize = round::floor((NUM_SERVERS / 2) as f64, 0) as usize;
}
