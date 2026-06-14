//! Row ↔ column transposition between `Event`/`EventPage` and
//! `Datum`/`DatumPage`.
//!
//! Mirrors `event_model.pack_event_page` / `unpack_event_page` /
//! `pack_datum_page` / `unpack_datum_page` (`__init__.py:2620-2751`).
//! Packing transposes a list of row documents into one column-store page;
//! unpacking reverses it. As in the reference, only keys present on a given
//! row contribute to that row's columns, and the first document's descriptor
//! / resource UID is used for the whole page.

use crate::documents::{Datum, DatumPage, Event, EventPage};
use serde_json::Value;
use std::collections::HashMap;

/// Transpose a slice of `Event`s into a single `EventPage`.
///
/// All events are assumed to share a descriptor (the first event's descriptor
/// UID labels the page). Returns an empty page if `events` is empty.
pub fn pack_event_page(events: &[Event]) -> EventPage {
    let descriptor = events
        .first()
        .map(|e| e.descriptor.clone())
        .unwrap_or_default();
    let mut uid = Vec::with_capacity(events.len());
    let mut time = Vec::with_capacity(events.len());
    let mut seq_num = Vec::with_capacity(events.len());
    let mut data: HashMap<String, Vec<Value>> = HashMap::new();
    let mut timestamps: HashMap<String, Vec<f64>> = HashMap::new();
    let mut filled: HashMap<String, Vec<Value>> = HashMap::new();
    for e in events {
        uid.push(e.uid.clone());
        time.push(e.time);
        seq_num.push(e.seq_num);
        for (k, v) in &e.data {
            data.entry(k.clone()).or_default().push(v.clone());
        }
        for (k, v) in &e.timestamps {
            timestamps.entry(k.clone()).or_default().push(*v);
        }
        for (k, v) in &e.filled {
            filled.entry(k.clone()).or_default().push(v.clone());
        }
    }
    EventPage {
        uid,
        descriptor,
        time,
        seq_num,
        data,
        timestamps,
        filled,
    }
}

/// Transpose an `EventPage` back into individual `Event`s, one per row.
pub fn unpack_event_page(page: &EventPage) -> Vec<Event> {
    (0..page.uid.len())
        .map(|i| {
            let data = page
                .data
                .iter()
                .filter_map(|(k, col)| col.get(i).map(|v| (k.clone(), v.clone())))
                .collect();
            let timestamps = page
                .timestamps
                .iter()
                .filter_map(|(k, col)| col.get(i).map(|v| (k.clone(), *v)))
                .collect();
            let filled = page
                .filled
                .iter()
                .filter_map(|(k, col)| col.get(i).map(|v| (k.clone(), v.clone())))
                .collect();
            Event {
                uid: page.uid[i].clone(),
                descriptor: page.descriptor.clone(),
                time: page.time[i],
                seq_num: page.seq_num[i],
                data,
                timestamps,
                filled,
            }
        })
        .collect()
}

/// Transpose a slice of `Datum`s into a single `DatumPage`.
///
/// All datums are assumed to share a resource (the first datum's resource UID
/// labels the page). Returns an empty page if `datums` is empty.
pub fn pack_datum_page(datums: &[Datum]) -> DatumPage {
    let resource = datums
        .first()
        .map(|d| d.resource.clone())
        .unwrap_or_default();
    let mut datum_id = Vec::with_capacity(datums.len());
    let mut datum_kwargs: HashMap<String, Vec<Value>> = HashMap::new();
    for d in datums {
        datum_id.push(d.datum_id.clone());
        for (k, v) in &d.datum_kwargs {
            datum_kwargs.entry(k.clone()).or_default().push(v.clone());
        }
    }
    DatumPage {
        datum_id,
        resource,
        datum_kwargs,
    }
}

/// Transpose a `DatumPage` back into individual `Datum`s, one per row.
pub fn unpack_datum_page(page: &DatumPage) -> Vec<Datum> {
    (0..page.datum_id.len())
        .map(|i| {
            let datum_kwargs = page
                .datum_kwargs
                .iter()
                .filter_map(|(k, col)| col.get(i).map(|v| (k.clone(), v.clone())))
                .collect();
            Datum {
                datum_id: page.datum_id[i].clone(),
                resource: page.resource.clone(),
                datum_kwargs,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(uid: &str, seq: u64, x: f64) -> Event {
        Event {
            uid: uid.into(),
            descriptor: "d-1".into(),
            time: 100.0 + seq as f64,
            seq_num: seq,
            data: HashMap::from([("x".into(), json!(x))]),
            timestamps: HashMap::from([("x".into(), 100.0 + seq as f64)]),
            filled: HashMap::new(),
        }
    }

    #[test]
    fn event_page_round_trip() {
        let events = vec![event("e-1", 1, 1.5), event("e-2", 2, 2.5)];
        let page = pack_event_page(&events);
        assert_eq!(page.uid, vec!["e-1", "e-2"]);
        assert_eq!(page.descriptor, "d-1");
        assert_eq!(page.data["x"], vec![json!(1.5), json!(2.5)]);
        let back = unpack_event_page(&page);
        assert_eq!(back, events);
    }

    #[test]
    fn datum_page_round_trip() {
        let datums = vec![
            Datum {
                datum_id: "r/1".into(),
                resource: "r".into(),
                datum_kwargs: HashMap::from([("i".into(), json!(1))]),
            },
            Datum {
                datum_id: "r/2".into(),
                resource: "r".into(),
                datum_kwargs: HashMap::from([("i".into(), json!(2))]),
            },
        ];
        let page = pack_datum_page(&datums);
        assert_eq!(page.datum_id, vec!["r/1", "r/2"]);
        assert_eq!(page.resource, "r");
        let back = unpack_datum_page(&page);
        assert_eq!(back, datums);
    }

    #[test]
    fn empty_event_page_is_safe() {
        let page = pack_event_page(&[]);
        assert!(page.uid.is_empty());
        assert!(unpack_event_page(&page).is_empty());
    }
}
