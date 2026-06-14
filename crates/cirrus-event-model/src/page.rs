//! Row ↔ column transposition between `Event`/`EventPage` and
//! `Datum`/`DatumPage`.
//!
//! Mirrors `event_model.pack_event_page` / `unpack_event_page` /
//! `pack_datum_page` / `unpack_datum_page` / `merge_event_pages`
//! (`__init__.py:2620-2862`).
//! Packing transposes a list of row documents into one column-store page;
//! unpacking reverses it; merging concatenates several pages into one. As in
//! the reference, only keys present on a given row contribute to that row's
//! columns, and the first document's descriptor / resource UID is used for the
//! whole page.

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

/// Combine a slice of `EventPage`s into one, concatenating every column.
///
/// Mirrors `event_model.merge_event_pages`. The first page's descriptor UID
/// labels the result. Column keys are unioned across all pages (like
/// [`pack_event_page`], rather than the reference's first-page-only key set)
/// and each page's values are appended in order; a page missing a key
/// contributes no rows to that column. Returns an empty page if `pages` is
/// empty, and the sole page (cloned) when there is exactly one.
pub fn merge_event_pages(pages: &[EventPage]) -> EventPage {
    if pages.len() == 1 {
        return pages[0].clone();
    }
    let descriptor = pages
        .first()
        .map(|p| p.descriptor.clone())
        .unwrap_or_default();
    let total: usize = pages.iter().map(|p| p.uid.len()).sum();
    let mut uid = Vec::with_capacity(total);
    let mut time = Vec::with_capacity(total);
    let mut seq_num = Vec::with_capacity(total);
    let mut data: HashMap<String, Vec<Value>> = HashMap::new();
    let mut timestamps: HashMap<String, Vec<f64>> = HashMap::new();
    let mut filled: HashMap<String, Vec<Value>> = HashMap::new();
    for p in pages {
        uid.extend(p.uid.iter().cloned());
        time.extend(p.time.iter().copied());
        seq_num.extend(p.seq_num.iter().copied());
        for (k, col) in &p.data {
            data.entry(k.clone())
                .or_default()
                .extend(col.iter().cloned());
        }
        for (k, col) in &p.timestamps {
            timestamps
                .entry(k.clone())
                .or_default()
                .extend(col.iter().copied());
        }
        for (k, col) in &p.filled {
            filled
                .entry(k.clone())
                .or_default()
                .extend(col.iter().cloned());
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

    #[test]
    fn merge_event_pages_concatenates_columns_and_keeps_first_descriptor() {
        let p1 = pack_event_page(&[event("e-1", 1, 1.5), event("e-2", 2, 2.5)]);
        let mut p2 = pack_event_page(&[event("e-3", 3, 3.5)]);
        // A differing descriptor on a later page must not win.
        p2.descriptor = "d-2".into();
        let merged = merge_event_pages(&[p1, p2]);
        assert_eq!(merged.uid, vec!["e-1", "e-2", "e-3"]);
        assert_eq!(merged.seq_num, vec![1, 2, 3]);
        assert_eq!(merged.time, vec![101.0, 102.0, 103.0]);
        assert_eq!(
            merged.data["x"],
            vec![json!(1.5), json!(2.5), json!(3.5)],
            "data column concatenated in page order"
        );
        assert_eq!(merged.timestamps["x"], vec![101.0, 102.0, 103.0]);
        assert_eq!(
            merged.descriptor, "d-1",
            "first page's descriptor labels the merge"
        );
        // The merged page unpacks back to the three original events.
        let back = unpack_event_page(&merged);
        assert_eq!(
            back,
            vec![
                event("e-1", 1, 1.5),
                event("e-2", 2, 2.5),
                event("e-3", 3, 3.5)
            ]
        );
    }

    #[test]
    fn merge_event_pages_single_returns_that_page() {
        let p1 = pack_event_page(&[event("e-1", 1, 1.5)]);
        assert_eq!(merge_event_pages(std::slice::from_ref(&p1)), p1);
    }

    #[test]
    fn merge_event_pages_empty_is_safe() {
        let merged = merge_event_pages(&[]);
        assert!(merged.uid.is_empty());
        assert_eq!(merged.descriptor, "");
        assert!(unpack_event_page(&merged).is_empty());
    }
}
