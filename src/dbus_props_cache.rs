//! Cached D-Bus property path (daemon mode only).
//!
//! Coexists with the default stateless design rather than replacing it (a fresh
//! proxy is built and its properties fetched live on every collection cycle): this
//! module keeps long-lived proxies with zbus's built-in property caching
//! (`CacheProperties::Lazily`) across collection cycles, so a property is fetched once
//! and then served from an in-memory cache — kept warm for free via zbus's internal
//! `PropertiesChanged` subscription — for as long as the proxy stays alive. This only
//! pays off across multiple daemon cycles; one-shot mode gets no benefit since the
//! process exits before any cache reuse can happen.
//!
//! `get_or_create_*` return an owned, cheaply-`Clone`d proxy rather than holding the
//! shared cache `RwLock` across the caller's subsequent property-read `.await` — the
//! lock here only ever protects the `HashMap` bookkeeping, never a live D-Bus call.
//! It's an `RwLock` rather than a `Mutex` because reads (the hit path) vastly
//! outnumber writes (the once-per-unit bootstrap path) once the cache is warm, so
//! concurrent readers never block each other.

use std::collections::{HashMap, HashSet};
use std::future::Future;

use tokio::sync::RwLock;
use zbus::zvariant::OwnedObjectPath;

use crate::dbus::zbus_service::ServiceProxy;
use crate::dbus::zbus_timer::TimerProxy;
use crate::dbus::zbus_unit::UnitProxy;

/// Cached proxies for a single unit, keyed by unit name in `UnitPropertyCache`.
/// `unit` is created eagerly and read directly by callers outside this module;
/// `service`/`timer` sub-proxies are added lazily, only when a caller actually
/// needs them for that unit, and are only ever touched via the `get_or_create_*`
/// functions below, so they stay private to this module.
#[derive(Clone)]
pub(crate) struct CachedUnitEntry {
    pub unit: UnitProxy<'static>,
    service: Option<ServiceProxy<'static>>,
    timer: Option<TimerProxy<'static>>,
}

pub(crate) type UnitPropertyCache = HashMap<String, CachedUnitEntry>;

/// Whether a `get_or_create_*` call served an already-warm proxy (zero new D-Bus
/// calls needed to reach it) or had to build one from scratch this call. Callers
/// tally this into `UnitsCollectionTimings::cache_hits` so cache efficacy is
/// directly observable in output stats, not just assumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheOutcome {
    Hit,
    Bootstrap,
}

/// Ensure `cache[unit_name]` has a live `Unit` proxy, building one if needed, and
/// return an owned clone of the cache entry.
///
/// Safe to call concurrently for the same `unit_name` from multiple tasks (the
/// per-unit loop in `units::parse_unit_state` does exactly this): the hit-check
/// and the bootstrap-insert are two separate lock acquisitions, so concurrent
/// callers can race between them on a cold cache. Harmless if it happens - the
/// losing task's freshly-built proxy is simply discarded in favor of whichever
/// entry `or_insert` finds already there, at the cost of one wasted proxy build
/// and an undercounted `cache_hits` for that call - never correctness-breaking.
/// The read path (overwhelmingly the common case once warm) takes a shared
/// `RwLock` read lock and doesn't contend with other readers at all.
pub(crate) async fn get_or_create_unit_proxy(
    cache: &RwLock<UnitPropertyCache>,
    connection: &zbus::Connection,
    unit_name: &str,
    object_path: &OwnedObjectPath,
) -> zbus::Result<(CachedUnitEntry, CacheOutcome)> {
    if let Some(entry) = cache.read().await.get(unit_name) {
        return Ok((entry.clone(), CacheOutcome::Hit));
    }

    let unit = UnitProxy::builder(connection)
        .cache_properties(zbus::proxy::CacheProperties::Lazily)
        .path(object_path.clone())?
        .build()
        .await?;

    let mut guard = cache.write().await;
    let entry = guard
        .entry(unit_name.to_string())
        .or_insert(CachedUnitEntry {
            unit,
            service: None,
            timer: None,
        });
    Ok((entry.clone(), CacheOutcome::Bootstrap))
}

/// Shared shape behind `get_or_create_service_proxy`/`get_or_create_timer_proxy`:
/// check for an already-cached sub-proxy, build one if missing, then stash it back
/// on the entry. `get`/`set` project `CachedUnitEntry` down to the one `Option<T>`
/// field each caller cares about; `build` is only invoked on a cache miss.
///
/// `caller` names the public function for the precondition message below - both
/// callers must run after `get_or_create_unit_proxy` has created the entry for
/// `unit_name`, since this only ever mutates an existing entry, never inserts one.
async fn get_or_create_sub_proxy<T, Get, Set, Build, Fut>(
    cache: &RwLock<UnitPropertyCache>,
    unit_name: &str,
    caller: &str,
    get: Get,
    set: Set,
    build: Build,
) -> zbus::Result<(T, CacheOutcome)>
where
    T: Clone,
    Get: Fn(&CachedUnitEntry) -> &Option<T>,
    Set: FnOnce(&mut CachedUnitEntry) -> &mut Option<T>,
    Build: FnOnce() -> Fut,
    Fut: Future<Output = zbus::Result<T>>,
{
    if let Some(value) = cache
        .read()
        .await
        .get(unit_name)
        .and_then(|e| get(e).clone())
    {
        return Ok((value, CacheOutcome::Hit));
    }

    let value = build().await?;

    let mut guard = cache.write().await;
    match guard.get_mut(unit_name) {
        Some(entry) => {
            set(entry).get_or_insert(value.clone());
        }
        None => debug_assert!(
            false,
            "{caller}({unit_name}) called before get_or_create_unit_proxy created its entry - \
             the freshly-built proxy below is returned to the caller but won't be cached, so \
             the next call will rebuild it"
        ),
    }
    Ok((value, CacheOutcome::Bootstrap))
}

/// Ensure the cached entry for `unit_name` has a `Service` sub-proxy, building one if
/// needed. Must be called after `get_or_create_unit_proxy` has created the entry.
pub(crate) async fn get_or_create_service_proxy(
    cache: &RwLock<UnitPropertyCache>,
    connection: &zbus::Connection,
    unit_name: &str,
    object_path: &OwnedObjectPath,
) -> zbus::Result<(ServiceProxy<'static>, CacheOutcome)> {
    get_or_create_sub_proxy(
        cache,
        unit_name,
        "get_or_create_service_proxy",
        |entry| &entry.service,
        |entry| &mut entry.service,
        || async {
            ServiceProxy::builder(connection)
                .cache_properties(zbus::proxy::CacheProperties::Lazily)
                .path(object_path.clone())?
                .build()
                .await
        },
    )
    .await
}

/// Ensure the cached entry for `unit_name` has a `Timer` sub-proxy, building one if
/// needed. Must be called after `get_or_create_unit_proxy` has created the entry.
pub(crate) async fn get_or_create_timer_proxy(
    cache: &RwLock<UnitPropertyCache>,
    connection: &zbus::Connection,
    unit_name: &str,
    object_path: &OwnedObjectPath,
) -> zbus::Result<(TimerProxy<'static>, CacheOutcome)> {
    get_or_create_sub_proxy(
        cache,
        unit_name,
        "get_or_create_timer_proxy",
        |entry| &entry.timer,
        |entry| &mut entry.timer,
        || async {
            TimerProxy::builder(connection)
                .cache_properties(zbus::proxy::CacheProperties::Lazily)
                .path(object_path.clone())?
                .build()
                .await
        },
    )
    .await
}

/// Unit names present in `cached` but absent from `current` — evicted because the
/// unit is no longer listed by systemd (stopped/transient unit gone, etc.).
///
/// This is the per-cycle correctness backstop: it runs regardless of whether the
/// `UnitRemoved`-signal-driven `evict_unit` below is also catching most removals
/// incrementally, since signals can be missed across a connection drop/reconnect.
pub(crate) fn unit_names_to_evict(
    cached: &HashSet<String>,
    current: &HashSet<String>,
) -> HashSet<String> {
    cached.difference(current).cloned().collect()
}

/// Drop a single unit's cache entry immediately, in reaction to systemd's
/// `UnitRemoved` signal (see the listener task spawned in `stat_collector`).
/// Tightens the *typical* staleness window between a unit disappearing and its
/// entry being dropped; `unit_names_to_evict` above remains the guarantee for
/// anything this misses.
pub(crate) async fn evict_unit(cache: &RwLock<UnitPropertyCache>, unit_name: &str) {
    cache.write().await.remove(unit_name);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unit_names_to_evict_empty_when_all_present() {
        let cached: HashSet<String> = ["a.service", "b.timer"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let current = cached.clone();
        assert!(unit_names_to_evict(&cached, &current).is_empty());
    }

    #[test]
    fn test_unit_names_to_evict_finds_removed_units() {
        let cached: HashSet<String> = ["a.service", "b.timer", "c.service"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let current: HashSet<String> = ["a.service", "c.service"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let evicted = unit_names_to_evict(&cached, &current);
        assert_eq!(evicted, HashSet::from(["b.timer".to_string()]));
    }

    #[test]
    fn test_unit_names_to_evict_new_units_not_evicted() {
        let cached: HashSet<String> = ["a.service"].iter().map(|s| s.to_string()).collect();
        let current: HashSet<String> = ["a.service", "new.service"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(unit_names_to_evict(&cached, &current).is_empty());
    }
}
