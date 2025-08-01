// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::Mutex;

use engine_rocks::{CompactedEventSender, RocksCompactedEvent};
use engine_traits::{KvEngine, RaftEngine};
use tikv_util::warn;

use crate::store::{StoreMsg, fsm::store::RaftRouter};

// raftstore v1's implementation
pub struct RaftRouterCompactedEventSender<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub router: Mutex<RaftRouter<EK, ER>>,
}

impl<EK, ER> CompactedEventSender for RaftRouterCompactedEventSender<EK, ER>
where
    EK: KvEngine<CompactedEvent = RocksCompactedEvent>,
    ER: RaftEngine,
{
    fn send(&self, event: RocksCompactedEvent) {
        let router = self.router.lock().unwrap();
        let event = StoreMsg::CompactedEvent(event);
        if let Err(e) = router.send_control(event) {
            warn!(
                "send compaction finished event to raftstore failed";
                "err" => ?e,
            );
        }
    }
}
