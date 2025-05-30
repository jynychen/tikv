// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{Ord, Ordering as CmpOrdering, PartialOrd, Reverse},
    collections::{BTreeMap, BinaryHeap},
    fmt,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicIsize, Ordering},
    },
    time::Duration,
};

use causal_ts::{CausalTsProvider, CausalTsProviderImpl};
use collections::{HashMap, HashMapEntry, HashSet};
use concurrency_manager::ConcurrencyManager;
use crossbeam::atomic::AtomicCell;
use engine_traits::KvEngine;
use fail::fail_point;
use futures::compat::Future01CompatExt;
use grpcio::Environment;
use kvproto::{
    cdcpb::{
        ChangeDataRequest, ClusterIdMismatch as ErrorClusterIdMismatch,
        Compatibility as ErrorCompatibility, DuplicateRequest as ErrorDuplicateRequest,
        Error as EventError, Event, Event_oneof_event, ResolvedTs,
    },
    kvrpcpb::ApiVersion,
    metapb::Region,
};
use online_config::{ConfigChange, OnlineConfig};
use pd_client::{Feature, PdClient};
use raftstore::{
    coprocessor::{CmdBatch, ObserveId},
    router::CdcHandle,
    store::fsm::store::StoreRegionMeta,
};
use resolved_ts::{LeadershipResolver, resolve_by_raft};
use security::SecurityManager;
use tikv::{
    config::{CdcConfig, ResolvedTsConfig},
    storage::{Statistics, kv::LocalTablets},
};
use tikv_util::{
    debug, defer, error, impl_display_as_debug, info,
    memory::MemoryQuota,
    mpsc::bounded,
    slow_log,
    sys::thread::ThreadBuildWrapper,
    time::{Instant, Limiter, SlowTimer},
    timer::SteadyTimer,
    warn,
    worker::{Runnable, RunnableWithTimer, ScheduleError, Scheduler},
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::Semaphore,
};
use txn_types::{Key, TimeStamp, TxnExtra, TxnExtraScheduler};

use crate::{
    CdcObserver, Error,
    channel::{CdcEvent, SendError},
    delegate::{Delegate, Downstream, DownstreamId, DownstreamState, MiniLock, on_init_downstream},
    initializer::Initializer,
    metrics::*,
    old_value::{OldValueCache, OldValueCallback},
    service::{Conn, ConnId, FeatureGate, RequestId, validate_kv_api},
};

const FEATURE_RESOLVED_TS_STORE: Feature = Feature::require(5, 0, 0);
const METRICS_FLUSH_INTERVAL: u64 = 1_000; // 1s

pub enum Deregister {
    Conn(ConnId),
    Request {
        conn_id: ConnId,
        request_id: RequestId,
    },
    Region {
        conn_id: ConnId,
        request_id: RequestId,
        region_id: u64,
    },
    Downstream {
        conn_id: ConnId,
        request_id: RequestId,
        region_id: u64,
        downstream_id: DownstreamId,
        err: Option<Error>,
    },
    Delegate {
        region_id: u64,
        observe_id: ObserveId,
        err: Error,
    },
}

impl_display_as_debug!(Deregister);

impl fmt::Debug for Deregister {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("Deregister");
        match self {
            Deregister::Conn(ref conn_id) => de
                .field("deregister", &"conn")
                .field("conn_id", conn_id)
                .finish(),
            Deregister::Request {
                ref conn_id,
                ref request_id,
            } => de
                .field("deregister", &"request")
                .field("conn_id", conn_id)
                .field("request_id", request_id)
                .finish(),
            Deregister::Region {
                ref conn_id,
                ref request_id,
                ref region_id,
            } => de
                .field("deregister", &"region")
                .field("conn_id", conn_id)
                .field("request_id", request_id)
                .field("region_id", region_id)
                .finish(),
            Deregister::Downstream {
                ref conn_id,
                ref request_id,
                ref region_id,
                ref downstream_id,
                ref err,
            } => de
                .field("deregister", &"downstream")
                .field("conn_id", conn_id)
                .field("request_id", request_id)
                .field("region_id", region_id)
                .field("downstream_id", downstream_id)
                .field("err", err)
                .finish(),
            Deregister::Delegate {
                ref region_id,
                ref observe_id,
                ref err,
            } => de
                .field("deregister", &"delegate")
                .field("region_id", region_id)
                .field("observe_id", observe_id)
                .field("err", err)
                .finish(),
        }
    }
}

type InitCallback = Box<dyn FnOnce() + Send>;

pub enum Validate {
    Region(u64, Box<dyn FnOnce(Option<&Delegate>) + Send>),
    OldValueCache(Box<dyn FnOnce(&OldValueCache) + Send>),
    UnresolvedRegion(Box<dyn FnOnce(usize) + Send>),
}

pub enum Task {
    Register {
        request: ChangeDataRequest,
        downstream: Downstream,
    },
    Deregister(Deregister),
    OpenConn {
        conn: Conn,
    },
    SetConnVersion {
        conn_id: ConnId,
        version: semver::Version,
        explicit_features: Vec<&'static str>,
    },
    MultiBatch {
        multi: Vec<CmdBatch>,
        old_value_cb: OldValueCallback,
    },
    MinTs {
        regions: Vec<u64>,
        min_ts: TimeStamp,
        current_ts: TimeStamp,
    },
    FinishScanLocks {
        observe_id: ObserveId,
        region: Region,
        locks: BTreeMap<Key, MiniLock>,
    },
    RegisterMinTsEvent {
        leader_resolver: LeadershipResolver,
        // The time at which the event actually occurred.
        event_time: Instant,
    },
    // The result of ChangeCmd should be returned from CDC Endpoint to ensure
    // the downstream switches to Normal after the previous commands was sunk.
    InitDownstream {
        region_id: u64,
        observe_id: ObserveId,
        downstream_id: DownstreamId,
        downstream_state: Arc<AtomicCell<DownstreamState>>,
        sink: crate::channel::Sink,
        build_resolver: Arc<AtomicBool>,
        // `incremental_scan_barrier` will be sent into `sink` to ensure all delta changes
        // are delivered to the downstream. And then incremental scan can start.
        incremental_scan_barrier: CdcEvent,
        cb: InitCallback,
    },
    TxnExtra(TxnExtra),
    Validate(Validate),
    ChangeConfig(ConfigChange),
}

impl_display_as_debug!(Task);

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("CdcTask");
        match self {
            Task::Register {
                ref request,
                ref downstream,
                ..
            } => de
                .field("type", &"register")
                .field("register request", request)
                .field("request", request)
                .field("id", &downstream.id)
                .field("conn_id", &downstream.conn_id)
                .finish(),
            Task::Deregister(deregister) => de
                .field("type", &"deregister")
                .field("deregister", deregister)
                .finish(),
            Task::OpenConn { ref conn } => de
                .field("type", &"open_conn")
                .field("conn_id", &conn.get_id())
                .finish(),
            Task::SetConnVersion {
                ref conn_id,
                ref version,
                ref explicit_features,
            } => de
                .field("type", &"set_conn_version")
                .field("conn_id", conn_id)
                .field("version", version)
                .field("explicit_features", explicit_features)
                .finish(),
            Task::MultiBatch { multi, .. } => de
                .field("type", &"multi_batch")
                .field("multi_batch", &multi.len())
                .finish(),
            Task::MinTs {
                ref min_ts,
                ref current_ts,
                ..
            } => de
                .field("type", &"mit_ts")
                .field("current_ts", current_ts)
                .field("min_ts", min_ts)
                .finish(),
            Task::FinishScanLocks {
                ref observe_id,
                ref region,
                ..
            } => de
                .field("type", &"finish_scan_locks")
                .field("observe_id", &observe_id)
                .field("region_id", &region.get_id())
                .finish(),
            Task::RegisterMinTsEvent { ref event_time, .. } => {
                de.field("event_time", &event_time).finish()
            }
            Task::InitDownstream {
                ref region_id,
                ref observe_id,
                ref downstream_id,
                ..
            } => de
                .field("type", &"init_downstream")
                .field("region_id", &region_id)
                .field("observe_id", &observe_id)
                .field("downstream", &downstream_id)
                .finish(),
            Task::TxnExtra(_) => de.field("type", &"txn_extra").finish(),
            Task::Validate(validate) => match validate {
                Validate::Region(region_id, _) => de.field("region_id", &region_id).finish(),
                Validate::OldValueCache(_) => de.finish(),
                Validate::UnresolvedRegion(_) => de.finish(),
            },
            Task::ChangeConfig(change) => de
                .field("type", &"change_config")
                .field("change", change)
                .finish(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct ResolvedRegion {
    region_id: u64,
    resolved_ts: TimeStamp,
}

impl PartialOrd for ResolvedRegion {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for ResolvedRegion {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.resolved_ts.cmp(&other.resolved_ts)
    }
}

#[derive(Default, Debug)]
pub(crate) struct ResolvedRegionHeap {
    // BinaryHeap is max heap, so we reverse order to get a min heap.
    heap: BinaryHeap<Reverse<ResolvedRegion>>,
}

impl ResolvedRegionHeap {
    pub(crate) fn push(&mut self, region_id: u64, resolved_ts: TimeStamp) {
        self.heap.push(Reverse(ResolvedRegion {
            region_id,
            resolved_ts,
        }))
    }

    // Pop slow regions and the minimum resolved ts among them.
    fn pop(&mut self, count: usize) -> (TimeStamp, HashSet<u64>) {
        let mut min_resolved_ts = TimeStamp::max();
        let mut outliers = HashSet::with_capacity_and_hasher(count, Default::default());
        for _ in 0..count {
            if let Some(resolved_region) = self.heap.pop() {
                outliers.insert(resolved_region.0.region_id);
                if min_resolved_ts > resolved_region.0.resolved_ts {
                    min_resolved_ts = resolved_region.0.resolved_ts;
                }
            } else {
                break;
            }
        }
        (min_resolved_ts, outliers)
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

#[derive(Default, Debug)]
pub(crate) struct Advance {
    // multiplexing means one region can be subscribed multiple times in one `Conn`,
    // in which case progresses are grouped by (ConnId, request_id).
    pub(crate) multiplexing: HashMap<(ConnId, RequestId), ResolvedRegionHeap>,

    // exclusive means one region can only be subscribed one time in one `Conn`,
    // in which case progresses are grouped by ConnId.
    pub(crate) exclusive: HashMap<ConnId, ResolvedRegionHeap>,

    // To be compatible with old TiCDC client before v4.0.8.
    // TODO(qupeng): we can deprecate support for too old TiCDC clients.
    // map[(ConnId, region_id)]->(request_id, ts).
    pub(crate) compat: HashMap<(ConnId, u64), (RequestId, TimeStamp)>,

    pub(crate) scan_finished: usize,

    pub(crate) blocked_on_scan: usize,

    pub(crate) blocked_on_locks: usize,

    min_resolved_ts: u64,
    min_ts_region_id: u64,
}

impl Advance {
    fn emit_resolved_ts(&mut self, connections: &HashMap<ConnId, Conn>) {
        let handle_send_result = |conn: &Conn, res: Result<(), SendError>| match res {
            Ok(_) => {}
            Err(SendError::Disconnected) => {
                debug!("cdc send event failed, disconnected";
                        "conn_id" => ?conn.get_id(), "downstream" => ?conn.get_peer());
            }
            Err(SendError::Full) | Err(SendError::Congested) => {
                info!("cdc send event failed, full";
                        "conn_id" => ?conn.get_id(), "downstream" => ?conn.get_peer());
            }
        };

        let mut batch_min_resolved_ts = 0;
        let mut batch_min_ts_region_id = 0;
        let mut batch_send = |ts: u64, conn: &Conn, req_id: RequestId, regions: Vec<u64>| {
            if batch_min_resolved_ts == 0 || batch_min_resolved_ts > ts {
                batch_min_resolved_ts = ts;
                if !regions.is_empty() {
                    batch_min_ts_region_id = regions[0];
                }
            }

            let mut resolved_ts = ResolvedTs::default();
            resolved_ts.ts = ts;
            resolved_ts.request_id = req_id.0;
            *resolved_ts.mut_regions() = regions;

            let res = conn
                .get_sink()
                .unbounded_send(CdcEvent::ResolvedTs(resolved_ts), false);
            handle_send_result(conn, res);
        };

        let mut compat_min_resolved_ts = 0;
        let mut compat_min_ts_region_id = 0;
        let mut compat_send = |ts: u64, conn: &Conn, region_id: u64, req_id: RequestId| {
            if compat_min_resolved_ts == 0 || compat_min_resolved_ts > ts {
                compat_min_resolved_ts = ts;
                compat_min_ts_region_id = region_id;
            }

            let event = Event {
                region_id,
                request_id: req_id.0,
                event: Some(Event_oneof_event::ResolvedTs(ts)),
                ..Default::default()
            };
            let res = conn
                .get_sink()
                .unbounded_send(CdcEvent::Event(event), false);
            handle_send_result(conn, res);
        };

        let multiplexing = std::mem::take(&mut self.multiplexing).into_iter();
        let exclusive = std::mem::take(&mut self.exclusive).into_iter();
        let unioned = multiplexing
            .map(|((a, b), c)| (a, b, c))
            .chain(exclusive.map(|(a, c)| (a, RequestId(0), c)));

        for (conn_id, req_id, mut region_ts_heap) in unioned {
            let conn = connections.get(&conn_id).unwrap();
            let mut batch_count = 8;
            while !region_ts_heap.is_empty() {
                let (ts, regions) = region_ts_heap.pop(batch_count);
                if conn.features().contains(FeatureGate::BATCH_RESOLVED_TS) {
                    batch_send(ts.into_inner(), conn, req_id, Vec::from_iter(regions));
                }
                batch_count *= 4;
            }
        }

        for ((conn_id, region_id), (req_id, ts)) in std::mem::take(&mut self.compat) {
            let conn = connections.get(&conn_id).unwrap();
            compat_send(ts.into_inner(), conn, region_id, req_id);
        }

        if batch_min_resolved_ts > 0 {
            self.min_resolved_ts = batch_min_resolved_ts;
            self.min_ts_region_id = batch_min_ts_region_id;
        } else {
            self.min_resolved_ts = compat_min_resolved_ts;
            self.min_ts_region_id = compat_min_ts_region_id;
        }
    }
}

pub struct Endpoint<T, E, S> {
    cluster_id: u64,

    capture_regions: HashMap<u64, Delegate>,
    connections: HashMap<ConnId, Conn>,
    scheduler: Scheduler<Task>,
    cdc_handle: T,
    tablets: LocalTablets<E>,
    observer: CdcObserver,

    pd_client: Arc<dyn PdClient>,
    timer: SteadyTimer,
    tso_worker: Runtime,
    store_meta: Arc<StdMutex<S>>,
    /// The concurrency manager for transactions. It's needed for CDC to check
    /// locks when calculating resolved_ts.
    concurrency_manager: ConcurrencyManager,

    raftstore_v2: bool,
    config: CdcConfig,
    resolved_ts_config: ResolvedTsConfig,
    api_version: ApiVersion,

    // Incremental scan stuffs.
    workers: Runtime,
    scan_task_counter: Arc<AtomicIsize>,
    scan_concurrency_semaphore: Arc<Semaphore>,
    scan_speed_limiter: Limiter,
    fetch_speed_limiter: Limiter,
    max_scan_batch_bytes: usize,
    max_scan_batch_size: usize,
    sink_memory_quota: Arc<MemoryQuota>,

    old_value_cache: OldValueCache,

    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,

    // Metrics and logging.
    current_ts: TimeStamp,
    min_resolved_ts: TimeStamp,
    min_ts_region_id: u64,
    resolved_region_count: usize,
    unresolved_region_count: usize,
}

impl<T: 'static + CdcHandle<E>, E: KvEngine, S: StoreRegionMeta> Endpoint<T, E, S> {
    pub fn new(
        cluster_id: u64,
        config: &CdcConfig,
        resolved_ts_config: &ResolvedTsConfig,
        raftstore_v2: bool,
        api_version: ApiVersion,
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        cdc_handle: T,
        tablets: LocalTablets<E>,
        observer: CdcObserver,
        store_meta: Arc<StdMutex<S>>,
        concurrency_manager: ConcurrencyManager,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        sink_memory_quota: Arc<MemoryQuota>,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,
    ) -> Endpoint<T, E, S> {
        let workers = Builder::new_multi_thread()
            .thread_name("cdcwkr")
            .worker_threads(config.incremental_scan_threads)
            .with_sys_hooks()
            .build()
            .unwrap();
        let tso_worker = Builder::new_multi_thread()
            .thread_name("tso")
            .worker_threads(config.tso_worker_threads)
            .enable_time()
            .with_sys_hooks()
            .build()
            .unwrap();

        // Initialized for the first time, subsequent adjustments will be made based on
        // configuration updates.
        let scan_concurrency_semaphore =
            Arc::new(Semaphore::new(config.incremental_scan_concurrency));
        let old_value_cache = OldValueCache::new(config.old_value_cache_memory_quota);
        let scan_speed_limiter = Limiter::new(if config.incremental_scan_speed_limit.0 > 0 {
            config.incremental_scan_speed_limit.0 as f64
        } else {
            f64::INFINITY
        });
        let fetch_speed_limiter = Limiter::new(if config.incremental_fetch_speed_limit.0 > 0 {
            config.incremental_fetch_speed_limit.0 as f64
        } else {
            f64::INFINITY
        });

        CDC_SINK_CAP.set(sink_memory_quota.capacity() as i64);
        // For scan efficiency, the scan batch bytes should be around 1MB.
        let max_scan_batch_bytes = 1024 * 1024;
        // Assume 1KB per entry.
        let max_scan_batch_size = 1024;

        let region_read_progress = store_meta.lock().unwrap().region_read_progress().clone();
        let store_resolver_gc_interval = Duration::from_secs(60);
        let leader_resolver = LeadershipResolver::new(
            store_meta.lock().unwrap().store_id(),
            pd_client.clone(),
            env,
            security_mgr,
            region_read_progress,
            store_resolver_gc_interval,
        );
        let ep = Endpoint {
            cluster_id,

            capture_regions: HashMap::default(),
            connections: HashMap::default(),
            scheduler,
            cdc_handle,
            tablets,
            observer,

            pd_client,
            timer: SteadyTimer::default(),
            tso_worker,
            store_meta,
            concurrency_manager,

            raftstore_v2,
            config: config.clone(),
            resolved_ts_config: resolved_ts_config.clone(),
            api_version,

            workers,
            scan_task_counter: Arc::default(),
            scan_concurrency_semaphore,
            scan_speed_limiter,
            fetch_speed_limiter,
            max_scan_batch_bytes,
            max_scan_batch_size,
            sink_memory_quota,

            old_value_cache,
            causal_ts_provider,

            current_ts: TimeStamp::zero(),
            min_resolved_ts: TimeStamp::max(),
            min_ts_region_id: 0,
            resolved_region_count: 0,
            unresolved_region_count: 0,
        };
        ep.register_min_ts_event(leader_resolver, Instant::now());
        ep
    }

    fn on_change_cfg(&mut self, change: ConfigChange) {
        // Validate first.
        let mut validate_cfg = self.config.clone();
        if let Err(e) = validate_cfg.update(change) {
            warn!("cdc config update failed"; "error" => ?e);
            return;
        }
        if let Err(e) = validate_cfg.validate(self.raftstore_v2) {
            warn!("cdc config update failed"; "error" => ?e);
            return;
        }
        let change = self.config.diff(&validate_cfg);
        info!(
            "cdc config updated";
            "current_config" => ?self.config,
            "change" => ?change
        );
        // Update the config here. The following adjustments will all use the new
        // values.
        self.config.update(change.clone()).unwrap();

        // Maybe the cache will be lost due to smaller capacity,
        // but it is acceptable.
        if change.contains_key("old_value_cache_memory_quota") {
            self.old_value_cache
                .resize(self.config.old_value_cache_memory_quota);
        }

        // Maybe the limit will be exceeded for a while after the concurrency becomes
        // smaller, but it is acceptable.
        if change.contains_key("incremental_scan_concurrency") {
            self.scan_concurrency_semaphore =
                Arc::new(Semaphore::new(self.config.incremental_scan_concurrency))
        }

        if change.contains_key("sink_memory_quota") {
            self.sink_memory_quota
                .set_capacity(self.config.sink_memory_quota.0 as usize);
            CDC_SINK_CAP.set(self.sink_memory_quota.capacity() as i64);
        }

        if change.contains_key("incremental_scan_speed_limit") {
            let new_speed_limit = if self.config.incremental_scan_speed_limit.0 > 0 {
                self.config.incremental_scan_speed_limit.0 as f64
            } else {
                f64::INFINITY
            };

            self.scan_speed_limiter.set_speed_limit(new_speed_limit);
        }
        if change.contains_key("incremental_fetch_speed_limit") {
            let new_speed_limit = if self.config.incremental_fetch_speed_limit.0 > 0 {
                self.config.incremental_fetch_speed_limit.0 as f64
            } else {
                f64::INFINITY
            };

            self.fetch_speed_limiter.set_speed_limit(new_speed_limit);
        }
    }

    pub fn set_max_scan_batch_size(&mut self, max_scan_batch_size: usize) {
        self.max_scan_batch_size = max_scan_batch_size;
    }

    fn deregister_downstream(
        &mut self,
        region_id: u64,
        downstream_id: DownstreamId,
        err: Option<Error>,
    ) {
        let mut delegate = match self.capture_regions.entry(region_id) {
            HashMapEntry::Vacant(_) => return,
            HashMapEntry::Occupied(x) => x,
        };
        if delegate.get_mut().unsubscribe(downstream_id, err) {
            let observe_id = delegate.get().handle.id;
            delegate.remove();
            self.deregister_observe(region_id, observe_id);
        }
    }

    fn deregister_observe(&mut self, region_id: u64, observe_id: ObserveId) {
        let oid = self.observer.unsubscribe_region(region_id, observe_id);
        assert!(
            oid.is_some(),
            "unsubscribe region {} failed, ObserveId {:?}",
            region_id,
            observe_id,
        );
    }

    fn on_deregister(&mut self, deregister: Deregister) {
        info!("cdc deregister"; "deregister" => ?deregister);
        fail_point!("cdc_before_handle_deregister", |_| {});
        match deregister {
            Deregister::Conn(conn_id) => {
                let conn = self.connections.remove(&conn_id).unwrap();
                conn.iter_downstreams(|_, region_id, downstream_id, _| {
                    self.deregister_downstream(region_id, downstream_id, None);
                });
            }
            Deregister::Request {
                conn_id,
                request_id,
            } => {
                let conn = self.connections.get_mut(&conn_id).unwrap();
                for (region_id, downstream) in conn.unsubscribe_request(request_id) {
                    let err = Some(Error::Other("region not found".into()));
                    self.deregister_downstream(region_id, downstream, err);
                }
            }
            Deregister::Region {
                conn_id,
                request_id,
                region_id,
            } => {
                let conn = self.connections.get_mut(&conn_id).unwrap();
                if let Some(downstream) = conn.unsubscribe(request_id, region_id) {
                    let err = Some(Error::Other("region not found".into()));
                    self.deregister_downstream(region_id, downstream, err);
                }
            }
            Deregister::Downstream {
                conn_id,
                request_id,
                region_id,
                downstream_id,
                err,
            } => {
                let conn = match self.connections.get_mut(&conn_id) {
                    Some(conn) => conn,
                    None => return,
                };
                if let Some(new_downstream_id) = conn.get_downstream(request_id, region_id) {
                    // To avoid ABA problem, we must check the unique DownstreamId.
                    if new_downstream_id == downstream_id {
                        conn.unsubscribe(request_id, region_id);
                        self.deregister_downstream(region_id, downstream_id, err);
                    }
                }
            }
            Deregister::Delegate {
                region_id,
                observe_id,
                err,
            } => {
                let mut delegate = match self.capture_regions.entry(region_id) {
                    HashMapEntry::Vacant(_) => return,
                    HashMapEntry::Occupied(x) => {
                        // To avoid ABA problem, we must check the unique ObserveId.
                        if x.get().handle.id != observe_id {
                            return;
                        }
                        x.remove()
                    }
                };
                delegate.stop(err);
                for downstream in delegate.downstreams() {
                    let request_id = downstream.req_id;
                    for conn in &mut self.connections.values_mut() {
                        conn.unsubscribe(request_id, region_id);
                    }
                }
                self.deregister_observe(region_id, delegate.handle.id);
            }
        }
    }

    pub fn on_register(&mut self, mut request: ChangeDataRequest, mut downstream: Downstream) {
        let kv_api = request.get_kv_api();
        let api_version = self.api_version;
        let filter_loop = downstream.filter_loop;

        let region_id = request.region_id;
        let request_id = RequestId(request.request_id);
        let conn_id = downstream.conn_id;
        let downstream_id = downstream.id;
        let downstream_state = downstream.get_state();

        // The connection can be deregistered by some internal errors. Clients will
        // always be notified by closing the GRPC server stream, so it's OK to drop
        // the task directly.
        let conn = match self.connections.get_mut(&conn_id) {
            Some(conn) => conn,
            None => {
                info!("cdc register region on an deregistered connection, ignore";
                    "region_id" => region_id,
                    "conn_id" => ?conn_id,
                    "req_id" => ?request_id,
                    "downstream_id" => ?downstream_id);
                return;
            }
        };
        downstream.set_sink(conn.get_sink().clone());

        // Check if the cluster id matches if supported.
        if conn.features().contains(FeatureGate::VALIDATE_CLUSTER_ID) {
            let request_cluster_id = request.get_header().get_cluster_id();
            if self.cluster_id != request_cluster_id {
                let mut err_event = EventError::default();
                let mut err = ErrorClusterIdMismatch::default();
                err.set_current(self.cluster_id);
                err.set_request(request_cluster_id);
                err_event.set_cluster_id_mismatch(err);

                let _ = downstream.sink_error_event(region_id, err_event);
                return;
            }
        }

        if !validate_kv_api(kv_api, api_version) {
            error!("cdc RawKv is supported by api-version 2 only. TxnKv is not supported now.");
            let mut err_event = EventError::default();
            let mut err = ErrorCompatibility::default();
            err.set_required_version("6.2.0".to_string());
            err_event.set_compatibility(err);

            let _ = downstream.sink_error_event(region_id, err_event);
            return;
        }

        let scan_task_counter = self.scan_task_counter.clone();
        let scan_task_count = scan_task_counter.fetch_add(1, Ordering::Relaxed);
        let release_scan_task_counter = tikv_util::DeferContext::new(move || {
            scan_task_counter.fetch_sub(1, Ordering::Relaxed);
        });
        if scan_task_count >= self.config.incremental_scan_concurrency_limit as isize {
            debug!("cdc rejects registration, too many scan tasks";
                "region_id" => region_id,
                "conn_id" => ?conn_id,
                "req_id" => ?request_id,
                "scan_task_count" => scan_task_count,
                "incremental_scan_concurrency_limit" => self.config.incremental_scan_concurrency_limit,
            );
            // To avoid OOM (e.g., https://github.com/tikv/tikv/issues/16035),
            // TiKV needs to reject and return error immediately.
            let mut err_event = EventError::default();
            err_event.mut_server_is_busy().reason = "too many pending incremental scans".to_owned();
            let _ = downstream.sink_error_event(region_id, err_event);
            return;
        }

        let txn_extra_op = match self.store_meta.lock().unwrap().reader(region_id) {
            Some(reader) => reader.txn_extra_op.clone(),
            None => {
                warn!("cdc register for a not found region"; "region_id" => region_id);
                let mut err_event = EventError::default();
                err_event.mut_region_not_found().region_id = region_id;
                let _ = downstream.sink_error_event(region_id, err_event);
                return;
            }
        };

        if conn
            .subscribe(request_id, region_id, downstream_id, downstream_state)
            .is_some()
        {
            let mut err_event = EventError::default();
            let mut err = ErrorDuplicateRequest::default();
            err.set_region_id(region_id);
            err_event.set_duplicate_request(err);
            let _ = downstream.sink_error_event(region_id, err_event);
            error!("cdc duplicate register";
                "region_id" => region_id,
                "conn_id" => ?conn_id,
                "req_id" => ?request_id,
                "downstream_id" => ?downstream_id);
            return;
        }

        let mut is_new_delegate = false;
        let delegate = match self.capture_regions.entry(region_id) {
            HashMapEntry::Occupied(e) => e.into_mut(),
            HashMapEntry::Vacant(e) => {
                is_new_delegate = true;
                e.insert(Delegate::new(
                    region_id,
                    self.sink_memory_quota.clone(),
                    txn_extra_op,
                ))
            }
        };

        let observe_id = delegate.handle.id;
        info!("cdc register region";
            "region_id" => region_id,
            "conn_id" => ?conn.get_id(),
            "req_id" => ?request_id,
            "observe_id" => ?observe_id,
            "downstream_id" => ?downstream_id);

        let observed_range = downstream.observed_range.clone();
        let downstream_state = downstream.get_state();
        let sched = self.scheduler.clone();
        let scan_truncated = downstream.scan_truncated.clone();

        if let Err((err, downstream)) = delegate.subscribe(downstream) {
            let error_event = err.into_error_event(region_id);
            let _ = downstream.sink_error_event(region_id, error_event);
            conn.unsubscribe(request_id, region_id);
            if is_new_delegate {
                self.capture_regions.remove(&region_id);
            }
            return;
        }
        if is_new_delegate {
            // The region has never been registered.
            // Subscribe the change events of the region.
            let old_observe_id = self.observer.subscribe_region(region_id, observe_id);
            assert!(
                old_observe_id.is_none(),
                "region {} must not be observed twice, old ObserveId {:?}, new ObserveId {:?}",
                region_id,
                old_observe_id,
                observe_id
            );
        };

        let mut init = Initializer {
            region_id,
            conn_id,
            request_id,
            checkpoint_ts: request.checkpoint_ts.into(),
            region_epoch: request.take_region_epoch(),

            build_resolver: Arc::new(Default::default()),
            observed_range,
            observe_handle: delegate.handle.clone(),
            downstream_id,
            downstream_state,
            scan_truncated,

            tablet: self.tablets.get(region_id).map(|t| t.into_owned()),
            sched,
            sink: conn.get_sink().clone(),
            concurrency_semaphore: self.scan_concurrency_semaphore.clone(),

            scan_speed_limiter: self.scan_speed_limiter.clone(),
            fetch_speed_limiter: self.fetch_speed_limiter.clone(),
            max_scan_batch_bytes: self.max_scan_batch_bytes,
            max_scan_batch_size: self.max_scan_batch_size,

            ts_filter_ratio: self.config.incremental_scan_ts_filter_ratio,
            kv_api,
            filter_loop,
        };

        let cdc_handle = self.cdc_handle.clone();
        self.workers.spawn(async move {
            CDC_SCAN_TASKS.with_label_values(&["total"]).inc();
            match init.initialize(cdc_handle).await {
                Ok(()) => {
                    CDC_SCAN_TASKS.with_label_values(&["finish"]).inc();
                }
                Err(e) => {
                    CDC_SCAN_TASKS.with_label_values(&["abort"]).inc();
                    warn!(
                        "cdc initialize fail: {}", e; "region_id" => region_id,
                        "conn_id" => ?init.conn_id, "request_id" => ?init.request_id,
                    );
                    init.deregister_downstream(e)
                }
            }
            drop(release_scan_task_counter);
        });
    }

    pub fn on_multi_batch(&mut self, multi: Vec<CmdBatch>, old_value_cb: OldValueCallback) {
        fail_point!("cdc_before_handle_multi_batch", |_| {});
        let size = multi.iter().map(|b| b.size()).sum();
        self.sink_memory_quota.free(size);
        let mut statistics = Statistics::default();
        for batch in multi {
            let region_id = batch.region_id;
            let mut deregister = None;
            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                if delegate.has_failed() {
                    // Skip the batch if the delegate has failed.
                    continue;
                }
                if let Err(e) = delegate.on_batch(
                    batch,
                    &old_value_cb,
                    &mut self.old_value_cache,
                    &mut statistics,
                ) {
                    delegate.mark_failed();
                    // Delegate has error, deregister the delegate.
                    deregister = Some(Deregister::Delegate {
                        region_id,
                        observe_id: delegate.handle.id,
                        err: e,
                    });
                }
            }
            if let Some(deregister) = deregister {
                self.on_deregister(deregister);
            }
        }
        flush_oldvalue_stats(&statistics, TAG_DELTA_CHANGE);
    }

    fn finish_scan_locks(
        &mut self,
        observe_id: ObserveId,
        region: Region,
        locks: BTreeMap<Key, MiniLock>,
    ) {
        let region_id = region.get_id();
        match self.capture_regions.get_mut(&region_id) {
            None => {
                debug!("cdc region not found on region ready (finish scan locks)";
                    "region_id" => region.get_id());
            }
            Some(delegate) => {
                if delegate.handle.id != observe_id {
                    debug!("cdc stale region ready";
                        "region_id" => region.get_id(),
                        "observe_id" => ?observe_id,
                        "current_id" => ?delegate.handle.id);
                    return;
                }
                match delegate.finish_scan_locks(region, locks) {
                    Ok(fails) => {
                        let mut deregisters = Vec::new();
                        for (downstream, e) in fails {
                            deregisters.push(Deregister::Downstream {
                                conn_id: downstream.conn_id,
                                request_id: downstream.req_id,
                                region_id,
                                downstream_id: downstream.id,
                                err: Some(e),
                            });
                        }
                        // Deregister downstreams if there is any downstream fails to subscribe.
                        for deregister in deregisters {
                            self.on_deregister(deregister);
                        }
                    }
                    Err(e) => self.on_deregister(Deregister::Delegate {
                        region_id,
                        observe_id,
                        err: e,
                    }),
                }
            }
        }
    }

    fn on_min_ts(&mut self, regions: Vec<u64>, min_ts: TimeStamp, current_ts: TimeStamp) {
        self.current_ts = current_ts;
        self.min_resolved_ts = current_ts;

        let mut advance = Advance::default();
        for region_id in regions {
            if let Some(d) = self.capture_regions.get_mut(&region_id) {
                d.on_min_ts(min_ts, current_ts, &self.connections, &mut advance);
            }
        }

        self.resolved_region_count = advance.scan_finished;
        self.unresolved_region_count = advance.blocked_on_scan;
        advance.emit_resolved_ts(&self.connections);
        self.min_resolved_ts = advance.min_resolved_ts.into();
        self.min_ts_region_id = advance.min_ts_region_id;
    }

    fn register_min_ts_event(&self, mut leader_resolver: LeadershipResolver, event_time: Instant) {
        // Try to keep advance resolved ts every `min_ts_interval`, thus
        // the actual wait interval = `min_ts_interval` - the last register min_ts event
        // time.
        let interval = self
            .config
            .min_ts_interval
            .0
            .checked_sub(event_time.saturating_elapsed());
        let timeout = self.timer.delay(interval.unwrap_or_default());
        let pd_client = self.pd_client.clone();
        let scheduler = self.scheduler.clone();
        let cdc_handle = self.cdc_handle.clone();
        let regions: Vec<u64> = self.capture_regions.keys().copied().collect();
        let cm: ConcurrencyManager = self.concurrency_manager.clone();
        let hibernate_regions_compatible = self.config.hibernate_regions_compatible;
        let causal_ts_provider = self.causal_ts_provider.clone();
        // We use channel to deliver leader_resolver in async block.
        let (leader_resolver_tx, leader_resolver_rx) = bounded(1);
        let advance_ts_interval = self.resolved_ts_config.advance_ts_interval.0;

        let fut = async move {
            let _ = timeout.compat().await;
            // Ignore get tso errors since we will retry every `min_ts_interval`.
            let min_ts_pd = match causal_ts_provider {
                // TiKV API v2 is enabled when causal_ts_provider is Some.
                // In this scenario, get TSO from causal_ts_provider to make sure that
                // RawKV write requests will get larger TSO after this point.
                // RawKV CDC's resolved_ts is guaranteed by ConcurrencyManager::global_min_lock_ts,
                // which lock flying keys's ts in raw put and delete interfaces in `Storage`.
                Some(provider) => provider.async_get_ts().await.unwrap_or_default(),
                None => pd_client.get_tso().await.unwrap_or_default(),
            };
            let mut min_ts = min_ts_pd;

            // Sync with concurrency manager so that it can work correctly when
            // optimizations like async commit is enabled.
            // Note: This step must be done before scheduling `Task::MinTs` task, and the
            // resolver must be checked in or after `Task::MinTs`' execution.
            cm.update_max_ts(min_ts, "cdc").unwrap();
            if let Some(min_mem_lock_ts) = cm.global_min_lock_ts() {
                if min_mem_lock_ts < min_ts {
                    min_ts = min_mem_lock_ts;
                }
            }

            let slow_timer = SlowTimer::default();
            defer!({
                slow_log!(T slow_timer, "cdc resolve region leadership");
                if let Ok(leader_resolver) = leader_resolver_rx.try_recv() {
                    match scheduler.schedule(Task::RegisterMinTsEvent {
                        leader_resolver,
                        event_time: Instant::now(),
                    }) {
                        Ok(_) | Err(ScheduleError::Stopped(_)) => (),
                        // Must schedule `RegisterMinTsEvent` event otherwise resolved ts can not
                        // advance normally.
                        Err(err) => panic!("failed to register min ts event, error: {:?}", err),
                    }
                } else {
                    // During shutdown, tso runtime drops future immediately,
                    // leader_resolver may be lost when this future drops before
                    // delivering leader_resolver.
                    warn!("cdc leader resolver is lost, are we shutdown?");
                }
            });

            // Check region peer leadership, make sure they are leaders.
            let gate = pd_client.feature_gate();
            let regions =
                if hibernate_regions_compatible && gate.can_enable(FEATURE_RESOLVED_TS_STORE) {
                    CDC_RESOLVED_TS_ADVANCE_METHOD.set(1);
                    leader_resolver
                        .resolve(regions, min_ts, Some(advance_ts_interval))
                        .await
                } else {
                    CDC_RESOLVED_TS_ADVANCE_METHOD.set(0);
                    resolve_by_raft(regions, min_ts, cdc_handle).await
                };
            leader_resolver_tx.send(leader_resolver).unwrap();

            if !regions.is_empty() {
                match scheduler.schedule(Task::MinTs {
                    regions,
                    min_ts,
                    current_ts: min_ts_pd,
                }) {
                    Ok(_) | Err(ScheduleError::Stopped(_)) => (),
                    // Must schedule `MinTS` event otherwise resolved ts can not
                    // advance normally.
                    Err(err) => panic!("failed to schedule min ts event, error: {:?}", err),
                }
            }
        };
        self.tso_worker.spawn(fut);
    }

    fn on_open_conn(&mut self, conn: Conn) {
        self.connections.insert(conn.get_id(), conn);
    }

    fn on_set_conn_version(
        &mut self,
        conn_id: ConnId,
        version: semver::Version,
        explicit_features: Vec<&'static str>,
    ) {
        let conn = self.connections.get_mut(&conn_id).unwrap();
        conn.check_version_and_set_feature(version, explicit_features);
    }
}

impl<T: 'static + CdcHandle<E>, E: KvEngine, S: StoreRegionMeta + Send> Runnable
    for Endpoint<T, E, S>
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        debug!("cdc run task"; "task" => %task);

        match task {
            Task::MinTs {
                regions,
                min_ts,
                current_ts,
            } => self.on_min_ts(regions, min_ts, current_ts),
            Task::Register {
                request,
                downstream,
            } => self.on_register(request, downstream),
            Task::FinishScanLocks {
                observe_id,
                region,
                locks,
            } => self.finish_scan_locks(observe_id, region, locks),
            Task::Deregister(deregister) => self.on_deregister(deregister),
            Task::MultiBatch {
                multi,
                old_value_cb,
            } => self.on_multi_batch(multi, old_value_cb),
            Task::OpenConn { conn } => self.on_open_conn(conn),
            Task::SetConnVersion {
                conn_id,
                version,
                explicit_features,
            } => {
                self.on_set_conn_version(conn_id, version, explicit_features);
            }
            Task::RegisterMinTsEvent {
                leader_resolver,
                event_time,
            } => self.register_min_ts_event(leader_resolver, event_time),
            Task::InitDownstream {
                region_id,
                observe_id,
                downstream_id,
                downstream_state,
                sink,
                build_resolver,
                incremental_scan_barrier,
                cb,
            } => {
                match self.capture_regions.get_mut(&region_id) {
                    Some(delegate) if delegate.handle.id == observe_id => {
                        if delegate.init_lock_tracker() {
                            build_resolver.store(true, Ordering::Release);
                        }
                    }
                    _ => return,
                }
                if let Err(e) = sink.unbounded_send(incremental_scan_barrier, true) {
                    warn!("cdc failed to schedule barrier for delta before delta scan";
                        "region_id" => region_id,
                        "observe_id" => ?observe_id,
                        "downstream_id" => ?downstream_id,
                        "error" => ?e);
                    return;
                }
                if on_init_downstream(&downstream_state) {
                    info!("cdc downstream starts to initialize";
                        "region_id" => region_id,
                        "observe_id" => ?observe_id,
                        "downstream_id" => ?downstream_id);
                } else {
                    warn!("cdc downstream fails to initialize: canceled";
                        "region_id" => region_id,
                        "observe_id" => ?observe_id,
                        "downstream_id" => ?downstream_id);
                }
                cb();
            }
            Task::TxnExtra(txn_extra) => {
                let size = txn_extra.size();
                for (k, v) in txn_extra.old_values {
                    self.old_value_cache.insert(k, v);
                }
                self.sink_memory_quota.free(size);
            }
            Task::Validate(validate) => match validate {
                Validate::Region(region_id, validate) => {
                    validate(self.capture_regions.get(&region_id));
                }
                Validate::OldValueCache(validate) => {
                    validate(&self.old_value_cache);
                }
                Validate::UnresolvedRegion(validate) => {
                    validate(self.unresolved_region_count);
                }
            },
            Task::ChangeConfig(change) => self.on_change_cfg(change),
        }
    }
}

impl<T: 'static + CdcHandle<E>, E: KvEngine, S: StoreRegionMeta + Send> RunnableWithTimer
    for Endpoint<T, E, S>
{
    fn on_timeout(&mut self) {
        CDC_ENDPOINT_PENDING_TASKS.set(self.scheduler.pending_tasks() as _);
        CDC_CAPTURED_REGION_COUNT.set(self.capture_regions.len() as i64);
        CDC_REGION_RESOLVE_STATUS_GAUGE_VEC
            .with_label_values(&["unresolved"])
            .set(self.unresolved_region_count as _);
        CDC_REGION_RESOLVE_STATUS_GAUGE_VEC
            .with_label_values(&["resolved"])
            .set(self.resolved_region_count as _);

        if self.min_resolved_ts != TimeStamp::max() {
            CDC_MIN_RESOLVED_TS_REGION.set(self.min_ts_region_id as i64);
            CDC_MIN_RESOLVED_TS.set(self.min_resolved_ts.physical() as i64);
            CDC_MIN_RESOLVED_TS_LAG.set(
                self.current_ts
                    .physical()
                    .saturating_sub(self.min_resolved_ts.physical()) as i64,
            );
            CDC_RESOLVED_TS_GAP_HISTOGRAM.observe(
                self.current_ts
                    .physical()
                    .saturating_sub(self.min_resolved_ts.physical()) as f64
                    / 1000f64,
            );
        }
        self.min_resolved_ts = TimeStamp::max();
        self.current_ts = TimeStamp::max();
        self.min_ts_region_id = 0;

        self.old_value_cache.flush_metrics();
        CDC_SINK_BYTES.set(self.sink_memory_quota.in_use() as i64);
    }

    fn get_interval(&self) -> Duration {
        // Currently there is only one timeout for CDC.
        Duration::from_millis(METRICS_FLUSH_INTERVAL)
    }
}

pub struct CdcTxnExtraScheduler {
    scheduler: Scheduler<Task>,
    memory_quota: Arc<MemoryQuota>,
}

impl CdcTxnExtraScheduler {
    pub fn new(scheduler: Scheduler<Task>, memory_quota: Arc<MemoryQuota>) -> CdcTxnExtraScheduler {
        CdcTxnExtraScheduler {
            scheduler,
            memory_quota,
        }
    }
}

impl TxnExtraScheduler for CdcTxnExtraScheduler {
    fn schedule(&self, txn_extra: TxnExtra) {
        let size = txn_extra.size();
        if let Err(e) = self.memory_quota.alloc(size) {
            CDC_DROP_TXN_EXTRA_TASKS_COUNT.inc();
            debug!("cdc schedule txn extra failed on alloc memory quota";
                "in_use" => self.memory_quota.in_use(), "err" => ?e);
            return;
        }
        if let Err(e) = self.scheduler.schedule(Task::TxnExtra(txn_extra)) {
            error!("cdc schedule txn extra failed"; "err" => ?e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Deref, DerefMut};

    use engine_rocks::RocksEngine;
    use futures::executor::block_on;
    use kvproto::{
        cdcpb::{ChangeDataRequestKvApi, Header},
        errorpb::Error as ErrorHeader,
    };
    use raftstore::{
        errors::{DiscardReason, Error as RaftStoreError},
        router::{CdcRaftRouter, RaftStoreRouter},
        store::{PeerMsg, ReadDelegate, fsm::StoreMeta, msg::CasualMessage},
    };
    use test_pd_client::TestPdClient;
    use test_raftstore::MockRaftStoreRouter;
    use tikv::{
        server::DEFAULT_CLUSTER_ID,
        storage::{TestEngineBuilder, kv::Engine},
    };
    use tikv_util::{
        config::{ReadableDuration, ReadableSize},
        worker::{ReceiverWrapper, dummy_scheduler},
    };

    use super::*;
    use crate::{
        channel,
        delegate::{ObservedRange, post_init_downstream},
        recv_timeout,
    };

    fn set_conn_version_task(conn_id: ConnId, version: semver::Version) -> Task {
        Task::SetConnVersion {
            conn_id,
            version,
            explicit_features: vec![],
        }
    }

    struct TestEndpointSuite {
        // The order must ensure `endpoint` be dropped before other fields.
        endpoint: Endpoint<CdcRaftRouter<MockRaftStoreRouter>, RocksEngine, StoreMeta>,
        cdc_handle: CdcRaftRouter<MockRaftStoreRouter>,
        task_rx: ReceiverWrapper<Task>,
        raft_rxs: HashMap<u64, tikv_util::mpsc::Receiver<PeerMsg<RocksEngine>>>,
        leader_resolver: Option<LeadershipResolver>,
    }

    impl TestEndpointSuite {
        // It's important to matain raft receivers in `raft_rxs`, otherwise all cases
        // need to drop `endpoint` and `rx` in order manually.
        fn add_region(&mut self, region_id: u64, cap: usize) {
            let rx = self.cdc_handle.add_region(region_id, cap);
            self.raft_rxs.insert(region_id, rx);
            self.add_local_reader(region_id);
        }

        fn add_local_reader(&self, region_id: u64) {
            self.store_meta
                .lock()
                .unwrap()
                .readers
                .insert(region_id, ReadDelegate::mock(region_id));
        }

        fn fill_raft_rx(&self, region_id: u64) {
            let router = &self.cdc_handle;
            loop {
                match router.send_casual_msg(region_id, CasualMessage::ClearRegionSize) {
                    Ok(_) => continue,
                    Err(RaftStoreError::Transport(DiscardReason::Full)) => break,
                    _ => unreachable!(),
                }
            }
        }

        fn raft_rx(&self, region_id: u64) -> &tikv_util::mpsc::Receiver<PeerMsg<RocksEngine>> {
            self.raft_rxs.get(&region_id).unwrap()
        }
    }

    impl Deref for TestEndpointSuite {
        type Target = Endpoint<CdcRaftRouter<MockRaftStoreRouter>, RocksEngine, StoreMeta>;
        fn deref(&self) -> &Self::Target {
            &self.endpoint
        }
    }

    impl DerefMut for TestEndpointSuite {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.endpoint
        }
    }

    fn mock_endpoint(
        cfg: &CdcConfig,
        engine: Option<RocksEngine>,
        api_version: ApiVersion,
    ) -> TestEndpointSuite {
        mock_endpoint_with_ts_provider(cfg, engine, api_version, None)
    }

    fn mock_endpoint_with_ts_provider(
        cfg: &CdcConfig,
        engine: Option<RocksEngine>,
        api_version: ApiVersion,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,
    ) -> TestEndpointSuite {
        let (task_sched, task_rx) = dummy_scheduler();
        let cdc_handle = CdcRaftRouter(MockRaftStoreRouter::new());
        let mut store_meta = StoreMeta::new(0);
        store_meta.store_id = Some(1);
        let region_read_progress = store_meta.region_read_progress.clone();
        let pd_client = Arc::new(TestPdClient::new(0, true));
        let env = Arc::new(Environment::new(1));
        let security_mgr = Arc::new(SecurityManager::default());
        let store_resolver_gc_interval = Duration::from_secs(60);
        let leader_resolver = LeadershipResolver::new(
            1,
            pd_client.clone(),
            env.clone(),
            security_mgr.clone(),
            region_read_progress,
            store_resolver_gc_interval,
        );

        let memory_quota = Arc::new(MemoryQuota::new(usize::MAX));
        let ep = Endpoint::new(
            DEFAULT_CLUSTER_ID,
            cfg,
            &ResolvedTsConfig::default(),
            false,
            api_version,
            pd_client,
            task_sched.clone(),
            cdc_handle.clone(),
            LocalTablets::Singleton(engine.unwrap_or_else(|| {
                TestEngineBuilder::new()
                    .build_without_cache()
                    .unwrap()
                    .kv_engine()
                    .unwrap()
            })),
            CdcObserver::new(task_sched, memory_quota.clone()),
            Arc::new(StdMutex::new(store_meta)),
            ConcurrencyManager::new_for_test(1.into()),
            env,
            security_mgr,
            memory_quota,
            causal_ts_provider,
        );

        TestEndpointSuite {
            endpoint: ep,
            cdc_handle,
            task_rx,
            raft_rxs: HashMap::default(),
            leader_resolver: Some(leader_resolver),
        }
    }

    #[test]
    fn test_api_version_check() {
        let mut cfg = CdcConfig::default();
        // To make the case more stable.
        cfg.min_ts_interval = ReadableDuration(Duration::from_secs(1));

        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);
        suite.add_region(1, 100);
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, mut rx) = channel::channel(ConnId::default(), 1, quota);
        let mut rx = rx.drain();

        let conn = Conn::new(ConnId::default(), tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });
        suite.run(set_conn_version_task(
            conn_id,
            FeatureGate::batch_resolved_ts(),
        ));

        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.set_kv_api(ChangeDataRequestKvApi::TiDb);
        let region_epoch = req.get_region_epoch().clone();

        // Compatibility error.
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            RequestId(1),
            conn_id,
            ChangeDataRequestKvApi::RawKv,
            false,
            ObservedRange::default(),
        );
        req.set_kv_api(ChangeDataRequestKvApi::RawKv);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::Event(mut e) = cdc_event.0 {
            assert_eq!(e.region_id, 1);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_compatibility());
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        suite
            .task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        // Compatibility error.
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            RequestId(2),
            conn_id,
            ChangeDataRequestKvApi::TxnKv,
            false,
            ObservedRange::default(),
        );
        req.set_kv_api(ChangeDataRequestKvApi::TxnKv);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::Event(mut e) = cdc_event.0 {
            assert_eq!(e.region_id, 1);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_compatibility());
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        suite
            .task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        suite.api_version = ApiVersion::V2;
        // Compatibility error.
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch,
            RequestId(3),
            conn_id,
            ChangeDataRequestKvApi::TxnKv,
            false,
            ObservedRange::default(),
        );
        req.set_kv_api(ChangeDataRequestKvApi::TxnKv);
        suite.run(Task::Register {
            request: req,
            downstream,
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::Event(mut e) = cdc_event.0 {
            assert_eq!(e.region_id, 1);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_compatibility());
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        suite
            .task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();
    }

    #[test]
    fn test_change_endpoint_cfg() {
        let cfg = CdcConfig::default();
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V2);
        let ep = &mut suite.endpoint;

        // Modify min_ts_interval and hibernate_regions_compatible.
        {
            let mut updated_cfg = cfg.clone();
            {
                // Update it to 0, this will be an invalid change and will be lost.
                updated_cfg.min_ts_interval = ReadableDuration::secs(0);
            }
            let diff = cfg.diff(&updated_cfg);
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(ep.config.min_ts_interval, ReadableDuration::secs(1));
            assert_eq!(ep.config.hibernate_regions_compatible, true);

            {
                // update fields.
                updated_cfg.min_ts_interval = ReadableDuration::secs(100);
                updated_cfg.hibernate_regions_compatible = false
            }
            let diff = cfg.diff(&updated_cfg);
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(ep.config.min_ts_interval, ReadableDuration::secs(100));
            assert_eq!(ep.config.hibernate_regions_compatible, false);
        }

        // Modify old_value_cache_memory_quota.
        {
            let mut updated_cfg = cfg.clone();
            {
                updated_cfg.old_value_cache_memory_quota = ReadableSize::mb(1024);
            }
            let diff = cfg.diff(&updated_cfg);

            assert_eq!(
                ep.config.old_value_cache_memory_quota,
                ReadableSize::mb(512)
            );
            assert_eq!(
                ep.old_value_cache.capacity(),
                ReadableSize::mb(512).0 as usize
            );
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(
                ep.config.old_value_cache_memory_quota,
                ReadableSize::mb(1024)
            );
            assert_eq!(
                ep.old_value_cache.capacity(),
                ReadableSize::mb(1024).0 as usize
            );
        }

        // Modify incremental_scan_concurrency.
        {
            let mut updated_cfg = cfg.clone();
            {
                // Update it to be smaller than incremental_scan_threads,
                // which will be an invalid change and will modified to
                // incremental_scan_threads.
                updated_cfg.incremental_scan_concurrency = 2;
            }
            let diff = cfg.diff(&updated_cfg);
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(ep.config.incremental_scan_concurrency, 4);
            assert_eq!(ep.scan_concurrency_semaphore.available_permits(), 4);

            {
                // Correct update.
                updated_cfg.incremental_scan_concurrency = 8;
            }
            let diff = cfg.diff(&updated_cfg);
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(ep.config.incremental_scan_concurrency, 8);
            assert_eq!(ep.scan_concurrency_semaphore.available_permits(), 8);
        }

        // Modify sink_memory_quota.
        {
            let mut updated_cfg = cfg.clone();
            {
                updated_cfg.sink_memory_quota = ReadableSize::mb(1024);
            }
            let diff = cfg.diff(&updated_cfg);

            assert_eq!(ep.sink_memory_quota.capacity(), isize::MAX as usize);
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(ep.config.sink_memory_quota, ReadableSize::mb(1024));
            assert_eq!(
                ep.sink_memory_quota.capacity(),
                ReadableSize::mb(1024).0 as usize
            );
        }

        // Modify incremental_scan_speed_limit.
        {
            let mut updated_cfg = cfg.clone();
            {
                updated_cfg.incremental_scan_speed_limit = ReadableSize::mb(1024);
            }
            let diff = cfg.diff(&updated_cfg);

            assert_eq!(
                ep.config.incremental_scan_speed_limit,
                ReadableSize::mb(128)
            );
            assert!(
                (ep.scan_speed_limiter.speed_limit() - ReadableSize::mb(128).0 as f64).abs()
                    < f64::EPSILON
            );
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(
                ep.config.incremental_scan_speed_limit,
                ReadableSize::mb(1024)
            );
            assert!(
                (ep.scan_speed_limiter.speed_limit() - ReadableSize::mb(1024).0 as f64).abs()
                    < f64::EPSILON
            );
        }

        // Modify incremental_fetch_speed_limit.
        {
            let mut updated_cfg = cfg.clone();
            {
                updated_cfg.incremental_fetch_speed_limit = ReadableSize::mb(2048);
            }
            let diff = cfg.diff(&updated_cfg);

            assert_eq!(
                ep.config.incremental_fetch_speed_limit,
                ReadableSize::mb(512)
            );
            assert!(
                (ep.fetch_speed_limiter.speed_limit() - ReadableSize::mb(512).0 as f64).abs()
                    < f64::EPSILON
            );
            ep.run(Task::ChangeConfig(diff));
            assert_eq!(
                ep.config.incremental_fetch_speed_limit,
                ReadableSize::mb(2048)
            );
            assert!(
                (ep.fetch_speed_limiter.speed_limit() - ReadableSize::mb(2048).0 as f64).abs()
                    < f64::EPSILON
            );
        }
    }

    #[test]
    fn test_raftstore_is_busy() {
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, _rx) = channel::channel(ConnId::default(), 1, quota);
        let mut suite = mock_endpoint(&CdcConfig::default(), None, ApiVersion::V1);

        // Fill the channel.
        suite.add_region(1 /* region id */, 1 /* cap */);
        suite.fill_raft_rx(1);

        let conn = Conn::new(ConnId::default(), tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });
        suite.run(set_conn_version_task(
            conn_id,
            semver::Version::new(0, 0, 0),
        ));

        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch,
            RequestId(0),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.run(Task::Register {
            request: req,
            downstream,
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);

        for _ in 0..5 {
            if let Ok(Some(Task::Deregister(Deregister::Downstream {
                err: Some(Error::Request(err)),
                ..
            }))) = suite.task_rx.recv_timeout(Duration::from_secs(1))
            {
                assert!(!err.has_server_is_busy());
            }
        }
    }

    #[test]
    fn test_register() {
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);
        suite.add_region(1, 100);
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, mut rx) = channel::channel(ConnId::default(), 1, quota);
        let mut rx = rx.drain();

        let conn = Conn::new(ConnId::default(), tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });

        // Enable batch resolved ts in the test.
        let version = FeatureGate::batch_resolved_ts();
        suite.run(set_conn_version_task(conn_id, version));

        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.set_request_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            RequestId(1),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);
        suite
            .task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        // duplicate request error.
        req.set_request_id(1);
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch,
            RequestId(1),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::Event(mut e) = cdc_event.0 {
            assert_eq!(e.region_id, 1);
            assert_eq!(e.request_id, 1);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_duplicate_request());
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        assert_eq!(suite.endpoint.capture_regions.len(), 1);
        suite
            .task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        // The first scan task of a region is initiated in register, and when it
        // fails, it should send a deregister region task, otherwise the region
        // delegate does not have resolver.
        //
        // Test non-exist region in raft router.
        let mut req = ChangeDataRequest::default();
        req.set_region_id(100);
        req.set_request_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            RequestId(1),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.add_local_reader(100);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        // Region 100 is inserted into capture_regions.
        assert_eq!(suite.endpoint.capture_regions.len(), 2);
        let task = suite
            .task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap();
        match task.unwrap() {
            Task::Deregister(Deregister::Delegate { region_id, err, .. }) => {
                assert_eq!(region_id, 100);
                assert!(matches!(err, Error::Request(_)), "{:?}", err);
            }
            other => panic!("unexpected task {:?}", other),
        }

        // Test errors on CaptureChange message.
        req.set_region_id(101);
        req.set_request_id(1);
        suite.add_region(101, 100);
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch,
            RequestId(1),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.run(Task::Register {
            request: req,
            downstream,
        });
        // Drop CaptureChange message, it should cause scan task failure.
        let timeout = Duration::from_millis(100);
        let _ = suite.raft_rx(101).recv_timeout(timeout).unwrap();
        assert_eq!(suite.endpoint.capture_regions.len(), 3);
        let task = suite.task_rx.recv_timeout(timeout).unwrap();
        match task.unwrap() {
            Task::Deregister(Deregister::Downstream { region_id, err, .. }) => {
                assert_eq!(region_id, 101);
                assert!(matches!(err, Some(Error::Other(_))), "{:?}", err);
            }
            other => panic!("unexpected task {:?}", other),
        }
    }

    #[test]
    fn test_too_many_scan_tasks() {
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            incremental_scan_concurrency: 1,
            incremental_scan_concurrency_limit: 1,
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);

        // Pause scan task runtime.
        suite.endpoint.workers = Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let (pause_tx, pause_rx) = std::sync::mpsc::channel::<()>();
        suite.endpoint.workers.spawn(async move {
            let _ = pause_rx.recv();
        });

        suite.add_region(1, 100);
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, mut rx) = channel::channel(ConnId::default(), 1, quota);
        let mut rx = rx.drain();

        let conn = Conn::new(ConnId::default(), tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });

        // Enable batch resolved ts in the test.
        let version = FeatureGate::batch_resolved_ts();
        suite.run(set_conn_version_task(conn_id, version));

        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.set_request_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            RequestId(1),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);

        // Test too many scan tasks error.
        req.set_request_id(2);
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch,
            RequestId(2),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::Event(mut e) = cdc_event.0 {
            assert_eq!(e.region_id, 1);
            assert_eq!(e.request_id, 2);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_server_is_busy());
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }

        drop(pause_tx);
    }

    #[test]
    fn test_raw_causal_min_ts() {
        let sleep_interval = Duration::from_secs(1);
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(sleep_interval),
            ..Default::default()
        };
        let ts_provider: Arc<CausalTsProviderImpl> =
            Arc::new(causal_ts::tests::TestProvider::default().into());
        let start_ts = block_on(ts_provider.async_get_ts()).unwrap();
        let mut suite =
            mock_endpoint_with_ts_provider(&cfg, None, ApiVersion::V2, Some(ts_provider.clone()));
        let leader_resolver = suite.leader_resolver.take().unwrap();
        suite.run(Task::RegisterMinTsEvent {
            leader_resolver,
            event_time: Instant::now(),
        });
        suite
            .task_rx
            .recv_timeout(Duration::from_millis(1500))
            .unwrap()
            .unwrap();
        let end_ts = block_on(ts_provider.async_get_ts()).unwrap();
        assert!(end_ts.into_inner() > start_ts.next().into_inner()); // may trigger more than once.
    }

    #[test]
    fn test_feature_gate() {
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);
        suite.add_region(1, 100);

        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, mut rx) = channel::channel(ConnId::default(), 1, quota);
        let mut rx = rx.drain();
        let mut region = Region::default();
        region.set_id(1);
        let conn = Conn::new(ConnId::default(), tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });

        // Enable batch resolved ts in the test.
        let version = FeatureGate::batch_resolved_ts();
        suite.run(set_conn_version_task(conn_id, version));

        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            RequestId(0),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        downstream.get_state().store(DownstreamState::Normal);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        let observe_id = suite.endpoint.capture_regions[&1].handle.id;
        suite
            .capture_regions
            .get_mut(&1)
            .unwrap()
            .init_lock_tracker();
        suite.finish_scan_locks(observe_id, region.clone(), Default::default());
        suite.run(Task::MinTs {
            regions: vec![1],
            min_ts: TimeStamp::from(1),
            current_ts: TimeStamp::zero(),
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::ResolvedTs(r) = cdc_event.0 {
            assert_eq!(r.regions, vec![1]);
            assert_eq!(r.ts, 1);
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }

        // Register region 2 to the conn.
        req.set_region_id(2);
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            RequestId(0),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        downstream.get_state().store(DownstreamState::Normal);
        suite.add_region(2, 100);
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        region.set_id(2);
        let observe_id = suite.endpoint.capture_regions[&2].handle.id;
        suite
            .capture_regions
            .get_mut(&2)
            .unwrap()
            .init_lock_tracker();
        suite.finish_scan_locks(observe_id, region, Default::default());
        suite.run(Task::MinTs {
            regions: vec![1, 2],
            min_ts: TimeStamp::from(2),
            current_ts: TimeStamp::zero(),
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::ResolvedTs(mut r) = cdc_event.0 {
            r.regions.as_mut_slice().sort_unstable();
            assert_eq!(r.regions, vec![1, 2]);
            assert_eq!(r.ts, 2);
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }

        // Register region 3 to another conn which is not support batch resolved ts.
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, mut rx2) = channel::channel(ConnId::default(), 1, quota);
        let mut rx2 = rx2.drain();
        let mut region = Region::default();
        region.set_id(3);
        let conn = Conn::new(ConnId::default(), tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });
        suite.run(set_conn_version_task(
            conn_id,
            semver::Version::new(4, 0, 5),
        ));

        req.set_region_id(3);
        req.set_request_id(3);
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch,
            RequestId(3),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        downstream.get_state().store(DownstreamState::Normal);
        suite.add_region(3, 100);
        suite.run(Task::Register {
            request: req,
            downstream,
        });
        region.set_id(3);
        let observe_id = suite.endpoint.capture_regions[&3].handle.id;
        suite
            .capture_regions
            .get_mut(&3)
            .unwrap()
            .init_lock_tracker();
        suite.finish_scan_locks(observe_id, region, Default::default());
        suite.run(Task::MinTs {
            regions: vec![1, 2, 3],
            min_ts: TimeStamp::from(3),
            current_ts: TimeStamp::zero(),
        });
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::ResolvedTs(mut r) = cdc_event.0 {
            r.regions.as_mut_slice().sort_unstable();
            // Region 3 resolved ts must not be send to the first conn when
            // batch resolved ts is enabled.
            assert_eq!(r.regions, vec![1, 2]);
            assert_eq!(r.ts, 3);
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        let cdc_event = channel::recv_timeout(&mut rx2, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        if let CdcEvent::Event(mut e) = cdc_event.0 {
            assert_eq!(e.region_id, 3);
            assert_eq!(e.request_id, 3);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::ResolvedTs(ts) => {
                    assert_eq!(ts, 3);
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
    }

    #[test]
    fn test_deregister() {
        let mut suite = mock_endpoint(&CdcConfig::default(), None, ApiVersion::V1);
        suite.add_region(1, 100);
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, mut rx) = channel::channel(ConnId::default(), 1, quota);
        let mut rx = rx.drain();

        let conn = Conn::new(ConnId::default(), tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });
        suite.run(set_conn_version_task(
            conn_id,
            semver::Version::new(0, 0, 0),
        ));

        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            RequestId(0),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        let downstream_id = downstream.id;
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);

        let mut err_header = ErrorHeader::default();
        err_header.set_not_leader(Default::default());
        let deregister = Deregister::Downstream {
            conn_id,
            request_id: RequestId(0),
            region_id: 1,
            downstream_id,
            err: Some(Error::request(err_header.clone())),
        };
        suite.run(Task::Deregister(deregister));
        loop {
            let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
                .unwrap()
                .unwrap();
            if let CdcEvent::Event(mut e) = cdc_event.0 {
                let event = e.event.take().unwrap();
                match event {
                    Event_oneof_event::Error(err) => {
                        assert!(err.has_not_leader());
                        break;
                    }
                    other => panic!("unknown event {:?}", other),
                }
            }
        }
        assert_eq!(suite.endpoint.capture_regions.len(), 0);

        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            RequestId(0),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        let new_downstream_id = downstream.id;
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);

        let deregister = Deregister::Downstream {
            conn_id,
            request_id: RequestId(0),
            region_id: 1,
            downstream_id,
            err: Some(Error::request(err_header.clone())),
        };
        suite.run(Task::Deregister(deregister));
        channel::recv_timeout(&mut rx, Duration::from_millis(200)).unwrap_err();
        assert_eq!(suite.endpoint.capture_regions.len(), 1);

        let deregister = Deregister::Downstream {
            conn_id,
            request_id: RequestId(0),
            region_id: 1,
            downstream_id: new_downstream_id,
            err: Some(Error::request(err_header.clone())),
        };
        suite.run(Task::Deregister(deregister));
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        loop {
            if let CdcEvent::Event(mut e) = cdc_event.0 {
                let event = e.event.take().unwrap();
                match event {
                    Event_oneof_event::Error(err) => {
                        assert!(err.has_not_leader());
                        break;
                    }
                    other => panic!("unknown event {:?}", other),
                }
            }
        }
        assert_eq!(suite.endpoint.capture_regions.len(), 0);

        // Stale deregister should be filtered.
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch,
            RequestId(0),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.run(Task::Register {
            request: req,
            downstream,
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);
        let deregister = Deregister::Delegate {
            region_id: 1,
            // A stale ObserveId (different from the actual one).
            observe_id: ObserveId::new(),
            err: Error::request(err_header),
        };
        suite.run(Task::Deregister(deregister));
        match channel::recv_timeout(&mut rx, Duration::from_millis(500)) {
            Err(_) => (),
            Ok(other) => panic!("unknown event {:?}", other),
        }
        assert_eq!(suite.endpoint.capture_regions.len(), 1);
    }

    #[test]
    fn test_broadcast_resolved_ts() {
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);

        // Open two connections a and b, registers region 1, 2 to conn a and
        // region 3 to conn b.
        let mut conn_rxs = vec![];
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        for region_ids in [vec![1, 2], vec![3]] {
            let (tx, rx) = channel::channel(ConnId::default(), 1, quota.clone());
            conn_rxs.push(rx);
            let conn = Conn::new(ConnId::default(), tx, String::new());
            let conn_id = conn.get_id();
            suite.run(Task::OpenConn { conn });
            let version = FeatureGate::batch_resolved_ts();
            suite.run(set_conn_version_task(conn_id, version));

            for region_id in region_ids {
                suite.add_region(region_id, 100);
                let mut req_header = Header::default();
                req_header.set_cluster_id(0);
                let mut req = ChangeDataRequest::default();
                req.set_region_id(region_id);
                let region_epoch = req.get_region_epoch().clone();
                let downstream = Downstream::new(
                    "".to_string(),
                    region_epoch.clone(),
                    RequestId(0),
                    conn_id,
                    ChangeDataRequestKvApi::TiDb,
                    false,
                    ObservedRange::default(),
                );
                downstream.get_state().store(DownstreamState::Normal);
                suite.run(Task::Register {
                    request: req.clone(),
                    downstream,
                });
                let observe_id = suite.endpoint.capture_regions[&region_id].handle.id;
                let mut region = Region::default();
                region.set_id(region_id);
                suite
                    .capture_regions
                    .get_mut(&region_id)
                    .unwrap()
                    .init_lock_tracker();
                suite.finish_scan_locks(observe_id, region, Default::default());
            }
        }

        let assert_batch_resolved_ts = |drain: &mut channel::Drain,
                                        regions: Vec<u64>,
                                        resolved_ts: u64| {
            let cdc_event = channel::recv_timeout(&mut drain.drain(), Duration::from_millis(500))
                .unwrap()
                .unwrap();
            if let CdcEvent::ResolvedTs(r) = cdc_event.0 {
                assert_eq!(r.regions, regions);
                assert_eq!(r.ts, resolved_ts);
            } else {
                panic!("unknown cdc event {:?}", cdc_event);
            }
        };

        suite.run(Task::MinTs {
            regions: vec![1],
            min_ts: TimeStamp::from(1),
            current_ts: TimeStamp::zero(),
        });
        // conn a must receive a resolved ts that only contains region 1.
        assert_batch_resolved_ts(conn_rxs.get_mut(0).unwrap(), vec![1], 1);
        // conn b must not receive any messages.
        channel::recv_timeout(
            &mut conn_rxs.get_mut(0).unwrap().drain(),
            Duration::from_millis(100),
        )
        .unwrap_err();

        suite.run(Task::MinTs {
            regions: vec![1, 2],
            min_ts: TimeStamp::from(2),
            current_ts: TimeStamp::zero(),
        });
        // conn a must receive a resolved ts that contains region 1 and region 2.
        assert_batch_resolved_ts(conn_rxs.get_mut(0).unwrap(), vec![1, 2], 2);
        // conn b must not receive any messages.
        channel::recv_timeout(
            &mut conn_rxs.get_mut(1).unwrap().drain(),
            Duration::from_millis(100),
        )
        .unwrap_err();

        suite.run(Task::MinTs {
            regions: vec![1, 2, 3],
            min_ts: TimeStamp::from(3),
            current_ts: TimeStamp::zero(),
        });
        // conn a must receive a resolved ts that contains region 1 and region 2.
        assert_batch_resolved_ts(conn_rxs.get_mut(0).unwrap(), vec![1, 2], 3);
        // conn b must receive a resolved ts that contains region 3.
        assert_batch_resolved_ts(conn_rxs.get_mut(1).unwrap(), vec![3], 3);

        suite.run(Task::MinTs {
            regions: vec![1, 3],
            min_ts: TimeStamp::from(4),
            current_ts: TimeStamp::zero(),
        });
        // conn a must receive a resolved ts that only contains region 1.
        assert_batch_resolved_ts(conn_rxs.get_mut(0).unwrap(), vec![1], 4);
        // conn b must receive a resolved ts that contains region 3.
        assert_batch_resolved_ts(conn_rxs.get_mut(1).unwrap(), vec![3], 4);
    }

    // Suppose there are two Conn that capture the same region,
    // Region epoch = 2, Conn A with epoch = 2, Conn B with epoch = 1,
    // Conn A builds resolver successfully, but is disconnected before
    // scheduling resolver ready. Downstream in Conn A is unsubscribed.
    // When resolver ready is installed, downstream in Conn B is unsubscribed
    // too, because epoch not match.
    #[test]
    fn test_deregister_conn_then_delegate() {
        let mut suite = mock_endpoint(&CdcConfig::default(), None, ApiVersion::V1);
        suite.add_region(1, 100);
        let quota = Arc::new(MemoryQuota::new(usize::MAX));

        // Open conn a
        let (tx1, _rx1) = channel::channel(ConnId::default(), 1, quota.clone());
        let conn_a = Conn::new(ConnId::default(), tx1, String::new());
        let conn_id_a = conn_a.get_id();
        suite.run(Task::OpenConn { conn: conn_a });
        suite.run(set_conn_version_task(
            conn_id_a,
            semver::Version::new(0, 0, 0),
        ));

        // Open conn b
        let (tx2, mut rx2) = channel::channel(ConnId::default(), 1, quota);
        let mut rx2 = rx2.drain();
        let conn_b = Conn::new(ConnId::default(), tx2, String::new());
        let conn_id_b = conn_b.get_id();
        suite.run(Task::OpenConn { conn: conn_b });
        suite.run(set_conn_version_task(
            conn_id_b,
            semver::Version::new(0, 0, 0),
        ));

        // Register region 1 (epoch 2) at conn a.
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.mut_region_epoch().set_version(2);
        let region_epoch_2 = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch_2.clone(),
            RequestId(0),
            conn_id_a,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);
        let observe_id = suite.endpoint.capture_regions[&1].handle.id;

        // Register region 1 (epoch 1) at conn b.
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        req.mut_region_epoch().set_version(1);
        let region_epoch_1 = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch_1,
            RequestId(0),
            conn_id_b,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.endpoint.capture_regions.len(), 1);

        // Deregister conn a.
        suite.run(Task::Deregister(Deregister::Conn(conn_id_a)));
        assert_eq!(suite.endpoint.capture_regions.len(), 1);

        // Schedule resolver ready (resolver is built by conn a).
        let mut region = Region::default();
        region.id = 1;
        region.set_region_epoch(region_epoch_2);
        suite
            .capture_regions
            .get_mut(&1)
            .unwrap()
            .init_lock_tracker();
        suite.run(Task::FinishScanLocks {
            observe_id,
            region: region.clone(),
            locks: Default::default(),
        });

        // Deregister deletgate due to epoch not match for conn b.
        let mut epoch_not_match = ErrorHeader::default();
        epoch_not_match
            .mut_epoch_not_match()
            .mut_current_regions()
            .push(region);
        suite.run(Task::Deregister(Deregister::Delegate {
            region_id: 1,
            observe_id,
            err: Error::request(epoch_not_match),
        }));
        assert_eq!(suite.endpoint.capture_regions.len(), 0);

        let event = recv_timeout(&mut rx2, Duration::from_millis(100))
            .unwrap()
            .unwrap()
            .0;
        assert!(
            event.event().get_error().has_epoch_not_match(),
            "{:?}",
            event
        );
    }

    #[test]
    fn test_resolved_region_heap() {
        let mut heap = ResolvedRegionHeap {
            heap: BinaryHeap::new(),
        };
        heap.push(5, 5.into());
        heap.push(4, 4.into());
        heap.push(6, 6.into());
        heap.push(3, 3.into());

        let (ts, regions) = heap.pop(0);
        assert_eq!(ts, TimeStamp::max());
        assert!(regions.is_empty());

        let (ts, regions) = heap.pop(2);
        assert_eq!(ts, 3.into());
        assert_eq!(regions.len(), 2);
        assert!(regions.contains(&3));
        assert!(regions.contains(&4));

        // Pop outliers more then it has.
        let (ts, regions) = heap.pop(3);
        assert_eq!(ts, 5.into());
        assert_eq!(regions.len(), 2);
        assert!(regions.contains(&5));
        assert!(regions.contains(&6));

        let mut heap1 = ResolvedRegionHeap {
            heap: BinaryHeap::new(),
        };
        heap1.push(5, 5.into());
        heap1.push(4, 4.into());
        heap1.push(6, 6.into());
        heap1.push(3, 3.into());

        let (ts, regions) = heap1.pop(1);
        assert_eq!(ts, 3.into());
        assert_eq!(regions.len(), 1);
        assert!(regions.contains(&3));
    }

    #[test]
    fn test_on_min_ts() {
        let cfg = CdcConfig {
            // Disable automatic advance resolved ts during test.
            min_ts_interval: ReadableDuration(Duration::from_secs(1000)),
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, mut rx) = channel::channel(ConnId::default(), 1, quota);
        let mut rx = rx.drain();

        let conn = Conn::new(ConnId::default(), tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });
        // Enable batch resolved ts in the test.
        let version = FeatureGate::batch_resolved_ts();
        suite.run(set_conn_version_task(conn_id, version));

        let mut req_header = Header::default();
        req_header.set_cluster_id(0);

        let mut regions = vec![];
        for id in 1..4097 {
            regions.push(id);
            suite.add_region(id, 100);

            let mut req = ChangeDataRequest::default();
            req.set_region_id(id);
            let region_epoch = req.get_region_epoch().clone();
            let downstream = Downstream::new(
                "".to_string(),
                region_epoch.clone(),
                RequestId(0),
                conn_id,
                ChangeDataRequestKvApi::TiDb,
                false,
                ObservedRange::default(),
            );
            on_init_downstream(&downstream.get_state());
            post_init_downstream(&downstream.get_state());
            suite.run(Task::Register {
                request: req.clone(),
                downstream,
            });

            let mut locks = BTreeMap::<Key, MiniLock>::default();
            locks.insert(
                Key::from_encoded(vec![]),
                MiniLock::from_ts(TimeStamp::compose(0, id)),
            );
            let mut region = Region::default();
            region.id = id;
            region.set_region_epoch(region_epoch);
            suite
                .capture_regions
                .get_mut(&id)
                .unwrap()
                .init_lock_tracker();
            let failed = suite
                .capture_regions
                .get_mut(&id)
                .unwrap()
                .finish_scan_locks(region, locks)
                .unwrap();
            assert!(failed.is_empty());
        }
        suite
            .task_rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();

        suite.run(Task::MinTs {
            regions,
            min_ts: TimeStamp::compose(0, 4096),
            current_ts: TimeStamp::compose(0, 4096),
        });

        // There should be at least 3 resolved ts events.
        let mut last_resolved_ts = 0;
        let mut last_batch_count = 0;
        for _ in 0..3 {
            let event = recv_timeout(&mut rx, Duration::from_millis(100))
                .unwrap()
                .unwrap()
                .0;
            assert!(last_resolved_ts < event.resolved_ts().ts, "{:?}", event);
            assert!(
                last_batch_count < event.resolved_ts().regions.len(),
                "{:?}",
                event
            );
            last_resolved_ts = event.resolved_ts().ts;
            last_batch_count = event.resolved_ts().regions.len();
        }
    }

    #[test]
    fn test_register_deregister_with_multiplexing() {
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);
        suite.add_region(1, 100);
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, mut rx) = channel::channel(ConnId::default(), 1, quota);
        let mut rx = rx.drain();

        let conn = Conn::new(ConnId::default(), tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });

        let version = FeatureGate::batch_resolved_ts();
        suite.run(set_conn_version_task(conn_id, version));

        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();

        req.set_region_id(1);
        req.set_request_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            RequestId(1),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 1);

        // Subscribe one region with a different request_id is allowed.
        req.set_request_id(2);
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            RequestId(2),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 2);

        // Subscribe one region with a same request_id is not allowed.
        req.set_request_id(2);
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            RequestId(2),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 2);
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        let check = matches!(cdc_event.0, CdcEvent::Event(e) if {
            matches!(e.event, Some(Event_oneof_event::Error(ref err)) if {
                err.has_duplicate_request()
            })
        });
        assert!(check);

        // Deregister an unexist downstream.
        suite.run(Task::Deregister(Deregister::Downstream {
            conn_id,
            request_id: RequestId(1),
            region_id: 1,
            downstream_id: DownstreamId::new(),
            err: None,
        }));
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 2);

        // Deregister an unexist delegate.
        suite.run(Task::Deregister(Deregister::Delegate {
            region_id: 1,
            observe_id: ObserveId::new(),
            err: Error::Rocks("test error".to_owned()),
        }));
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 2);

        // Deregister an exist downstream.
        let downstream_id = suite.capture_regions[&1].downstreams()[0].id;
        suite.run(Task::Deregister(Deregister::Downstream {
            conn_id,
            request_id: RequestId(1),
            region_id: 1,
            downstream_id,
            err: Some(Error::Rocks("test error".to_owned())),
        }));
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 1);
        let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
            .unwrap()
            .unwrap();
        let check = matches!(cdc_event.0, CdcEvent::Event(e) if {
            matches!(e.event, Some(Event_oneof_event::Error(ref err)) if {
                err.has_region_not_found()
            })
        });
        assert!(check);

        // Subscribe one region with a different request_id is allowed.
        req.set_request_id(1);
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            RequestId(1),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.run(Task::Register {
            request: req.clone(),
            downstream,
        });
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 2);

        // Deregister an exist delegate.
        let observe_id = suite.capture_regions[&1].handle.id;
        suite.run(Task::Deregister(Deregister::Delegate {
            region_id: 1,
            observe_id,
            err: Error::Rocks("test error".to_owned()),
        }));
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 0);
        assert_eq!(suite.capture_regions.len(), 0);
        for _ in 0..2 {
            let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
                .unwrap()
                .unwrap();
            let check = matches!(cdc_event.0, CdcEvent::Event(e) if {
                matches!(e.event, Some(Event_oneof_event::Error(ref err)) if {
                    err.has_region_not_found()
                })
            });
            assert!(check);
        }

        // Resubscribe the region.
        for i in 1..=2 {
            req.set_request_id(i as _);
            let downstream = Downstream::new(
                "".to_string(),
                region_epoch.clone(),
                RequestId(i),
                conn_id,
                ChangeDataRequestKvApi::TiDb,
                false,
                ObservedRange::default(),
            );
            suite.run(Task::Register {
                request: req.clone(),
                downstream,
            });
            assert_eq!(suite.connections[&conn_id].downstreams_count(), i as usize);
        }

        // Deregister the request.
        suite.run(Task::Deregister(Deregister::Request {
            conn_id,
            request_id: RequestId(1),
        }));
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 1);
        suite.run(Task::Deregister(Deregister::Request {
            conn_id,
            request_id: RequestId(2),
        }));
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 0);
        assert_eq!(suite.capture_regions.len(), 0);
        for _ in 0..2 {
            let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
                .unwrap()
                .unwrap();
            let check = matches!(cdc_event.0, CdcEvent::Event(e) if {
                matches!(e.event, Some(Event_oneof_event::Error(ref err)) if {
                    err.has_region_not_found()
                })
            });
            assert!(check);
        }

        // Resubscribe the region.
        suite.add_region(2, 100);
        for i in 1..=2 {
            req.set_request_id(1);
            req.set_region_id(i);
            let downstream = Downstream::new(
                "".to_string(),
                region_epoch.clone(),
                RequestId(1),
                conn_id,
                ChangeDataRequestKvApi::TiDb,
                false,
                ObservedRange::default(),
            );
            suite.run(Task::Register {
                request: req.clone(),
                downstream,
            });
            assert_eq!(suite.connections[&conn_id].downstreams_count(), i as usize);
        }

        // Deregister regions one by one in the request.
        suite.run(Task::Deregister(Deregister::Region {
            conn_id,
            request_id: RequestId(1),
            region_id: 1,
        }));
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 1);
        assert_eq!(suite.capture_regions.len(), 1);

        suite.run(Task::Deregister(Deregister::Region {
            conn_id,
            request_id: RequestId(1),
            region_id: 2,
        }));
        assert_eq!(suite.connections[&conn_id].downstreams_count(), 0);
        assert_eq!(suite.capture_regions.len(), 0);

        for _ in 0..2 {
            let cdc_event = channel::recv_timeout(&mut rx, Duration::from_millis(500))
                .unwrap()
                .unwrap();
            let check = matches!(cdc_event.0, CdcEvent::Event(e) if {
                matches!(e.event, Some(Event_oneof_event::Error(ref err)) if {
                    err.has_region_not_found()
                })
            });
            assert!(check);
        }
    }

    #[test]
    fn test_register_after_connection_deregistered() {
        let cfg = CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        };
        let mut suite = mock_endpoint(&cfg, None, ApiVersion::V1);
        suite.add_region(1, 100);
        let quota = Arc::new(MemoryQuota::new(usize::MAX));
        let (tx, _rx) = channel::channel(ConnId::default(), 1, quota);

        let conn = Conn::new(ConnId::default(), tx, String::new());
        let conn_id = conn.get_id();
        suite.run(Task::OpenConn { conn });

        suite.run(Task::Deregister(Deregister::Conn(conn_id)));

        let mut req = ChangeDataRequest::default();

        req.set_region_id(1);
        req.set_request_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new(
            "".to_string(),
            region_epoch.clone(),
            RequestId(1),
            conn_id,
            ChangeDataRequestKvApi::TiDb,
            false,
            ObservedRange::default(),
        );
        suite.run(Task::Register {
            request: req,
            downstream,
        });
        assert!(suite.connections.is_empty());
    }
}
