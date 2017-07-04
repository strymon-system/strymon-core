#[deprecated]
pub use strymon_communication::{fetch, Network};
#[deprecated]
pub use strymon_communication::transport::*;
#[deprecated]
pub use strymon_communication::rpc as reqrep;

#[deprecated]
pub mod message {
    #[deprecated]
    pub use strymon_communication::message::MessageBuf;
    #[deprecated]
    pub mod buf {
        #[deprecated]
        pub use strymon_communication::message::MessageBuf;
    }
}
