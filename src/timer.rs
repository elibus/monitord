//! # timers module
//!
//! All timer related logic goes here. This will be hitting timer specific
//! dbus / varlink etc.

use struct_field_names_as_array::FieldNamesAsArray;
use thiserror::Error;
use tracing::error;
use zbus::zvariant::ObjectPath;

use crate::dbus_props::extract_property;
use crate::units::SystemdUnitStats;

#[derive(Error, Debug)]
pub enum MonitordTimerError {
    #[error("Timer D-Bus error: {0}")]
    ZbusError(#[from] zbus::Error),
}

#[derive(
    serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, FieldNamesAsArray, PartialEq,
)]

/// Per-timer unit metrics from the org.freedesktop.systemd1.Timer D-Bus interface.
/// Ref: <https://www.freedesktop.org/software/systemd/man/org.freedesktop.systemd1.html>
pub struct TimerStats {
    /// AccuracySec timer property in microseconds; systemd may coalesce timer firings within this window to save wakeups
    pub accuracy_usec: u64,
    /// Whether FixedRandomDelay= is set; when true, the random delay is stable across reboots for this timer
    pub fixed_random_delay: bool,
    /// Realtime timestamp (usec since epoch) when this timer last triggered its service unit
    pub last_trigger_usec: u64,
    /// Monotonic timestamp (usec since boot) when this timer last triggered its service unit
    pub last_trigger_usec_monotonic: u64,
    /// Monotonic timestamp (usec since boot) when this timer will next elapse
    pub next_elapse_usec_monotonic: u64,
    /// Realtime timestamp (usec since epoch) when this timer will next elapse
    pub next_elapse_usec_realtime: u64,
    /// Whether Persistent= is set; when true, missed timer runs (e.g. during downtime) are triggered on next boot
    pub persistent: bool,
    /// RandomizedDelaySec property in microseconds; a random delay up to this value is added before each trigger
    pub randomized_delay_usec: u64,
    /// Whether RemainAfterElapse= is set; when true, the timer stays loaded after all triggers have elapsed
    pub remain_after_elapse: bool,
    /// Realtime timestamp (usec since epoch) of the most recent state change of the triggered service unit
    pub service_unit_last_state_change_usec: u64,
    /// Monotonic timestamp (usec since boot) of the most recent state change of the triggered service unit
    pub service_unit_last_state_change_usec_monotonic: u64,
}

pub const TIMER_STATS_FIELD_NAMES: &[&str] = &TimerStats::FIELD_NAMES_AS_ARRAY;

pub async fn collect_timer_stats(
    connection: &zbus::Connection,
    stats: &mut SystemdUnitStats,
    unit: &crate::units::ListedUnit,
) -> Result<TimerStats, MonitordTimerError> {
    let mut timer_stats = TimerStats::default();

    // One GetAll call fetches the triggered unit's name (Unit) plus all 9
    // other Timer properties, replacing what used to be 10 individual
    // Properties.Get round trips.
    let timer_path = ObjectPath::from(unit.unit_object_path.clone());
    let timer_props = crate::dbus_props::get_all_properties(
        connection,
        &timer_path,
        "org.freedesktop.systemd1.Timer",
    )
    .await?;

    let service_unit: String = extract_property(&timer_props, "Unit")?;
    if service_unit.is_empty() {
        error!("{}: No service unit name found for timer.", unit.name);
    } else {
        // Get the object path of the service unit
        let mp = crate::dbus::zbus_systemd::ManagerProxy::builder(connection)
            .cache_properties(zbus::proxy::CacheProperties::No)
            .build()
            .await?;
        let service_unit_path = mp.get_unit(&service_unit).await?;
        // One GetAll call fetches both state-change timestamps instead of
        // two individual Properties.Get round trips.
        let service_unit_props = crate::dbus_props::get_all_properties(
            connection,
            &service_unit_path,
            "org.freedesktop.systemd1.Unit",
        )
        .await?;
        timer_stats.service_unit_last_state_change_usec =
            extract_property(&service_unit_props, "StateChangeTimestamp")?;
        timer_stats.service_unit_last_state_change_usec_monotonic =
            extract_property(&service_unit_props, "StateChangeTimestampMonotonic")?;
    }

    timer_stats.accuracy_usec = extract_property(&timer_props, "AccuracyUSec")?;
    timer_stats.fixed_random_delay = extract_property(&timer_props, "FixedRandomDelay")?;
    timer_stats.last_trigger_usec = extract_property(&timer_props, "LastTriggerUSec")?;
    timer_stats.last_trigger_usec_monotonic =
        extract_property(&timer_props, "LastTriggerUSecMonotonic")?;
    timer_stats.persistent = extract_property(&timer_props, "Persistent")?;
    timer_stats.next_elapse_usec_monotonic =
        extract_property(&timer_props, "NextElapseUSecMonotonic")?;
    timer_stats.next_elapse_usec_realtime =
        extract_property(&timer_props, "NextElapseUSecRealtime")?;
    timer_stats.randomized_delay_usec = extract_property(&timer_props, "RandomizedDelayUSec")?;
    timer_stats.remain_after_elapse = extract_property(&timer_props, "RemainAfterElapse")?;

    if timer_stats.persistent {
        stats.timer_persistent_units += 1;
    }

    if timer_stats.remain_after_elapse {
        stats.timer_remain_after_elapse += 1;
    }

    Ok(timer_stats)
}

/// Collect all timer stats via D-Bus and return them ready to merge into unit stats.
///
/// Used when unit stats were collected via varlink (which doesn't yet expose timer
/// properties) so that `timers.*`, `timer_persistent_units`, and
/// `timer_remain_after_elapse` match the D-Bus output.
pub async fn collect_all_timers_dbus(
    connection: &zbus::Connection,
    config: &crate::config::Config,
) -> anyhow::Result<crate::units::SystemdUnitStats> {
    use std::collections::HashMap;
    use tracing::debug;

    if !config.timers.enabled {
        return Ok(crate::units::SystemdUnitStats::default());
    }

    let p = crate::dbus::zbus_systemd::ManagerProxy::builder(connection)
        .cache_properties(zbus::proxy::CacheProperties::No)
        .build()
        .await?;
    let units = p.list_units().await?;

    let mut stats = crate::units::SystemdUnitStats::default();
    let mut timer_stats_map = HashMap::new();

    for unit_raw in units {
        let unit: crate::units::ListedUnit = unit_raw.into();
        if !unit.name.contains(".timer") {
            continue;
        }
        if config.timers.blocklist.contains(&unit.name) {
            debug!("Skipping timer stats for {} due to blocklist", &unit.name);
            continue;
        }
        if !config.timers.allowlist.is_empty() && !config.timers.allowlist.contains(&unit.name) {
            continue;
        }
        match collect_timer_stats(connection, &mut stats, &unit).await {
            Ok(ts) => {
                timer_stats_map.insert(unit.name.clone(), ts);
            }
            Err(err) => {
                error!("Failed to get {} stats: {:#?}", &unit.name, err);
            }
        }
    }

    stats.timer_stats = timer_stats_map;
    Ok(stats)
}
