pub const NUM_SERVERS: usize = 3;
pub const MIN_TIMEOUT: usize = 150;
pub const MAX_TIMEOUT: usize = 300;
pub const MESSAGE_LENGTH: usize = 1024;

const fn _MIN_QUORUM() -> usize {
    if NUM_SERVERS % 2 == 1 {
        (NUM_SERVERS + 1) / 2
    } else {
        NUM_SERVERS / 2 + 1
    }
}

pub const MAJORITY: usize = _MIN_QUORUM();
