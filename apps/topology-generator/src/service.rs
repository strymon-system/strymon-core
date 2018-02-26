use strymon_communication::rpc::{Name, Request};

use super::Entity;

/// A list of changes to the topology
pub type TopologyUpdate = Vec<(Entity, i32)>;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct FetchTopology(());

impl FetchTopology {
    pub fn new() -> Self {
        FetchTopology(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Fault {
    DisconnectRandomSwitch,
    DisconnectRandomLink,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, TypeName)]
#[repr(u8)]
pub enum TopologyService {
    FetchTopology = 1,
    InjectRandomFault = 2,
}

impl Name for TopologyService {
    type Discriminant = u8;

    fn discriminant(&self) -> Self::Discriminant {
        *self as u8
    }

    fn from_discriminant(value: &Self::Discriminant) -> Option<Self> {
        match *value {
            1 => Some(TopologyService::FetchTopology),
            2 => Some(TopologyService::InjectRandomFault),
            _ => None,
        }
    }
}

pub type Version = u64;

impl Request<TopologyService> for FetchTopology {
    const NAME: TopologyService = TopologyService::FetchTopology;
    type Success = (Version, TopologyUpdate);
    type Error = ();
}

impl Request<TopologyService> for Fault {
    const NAME: TopologyService = TopologyService::InjectRandomFault;
    type Success = ();
    type Error = ();
}
