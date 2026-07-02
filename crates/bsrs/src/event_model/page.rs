//! Row ↔ column transposition between `Event`/`EventPage` and
//! `Datum`/`DatumPage`.
//!
//! Mirrors `event_model.pack_event_page` / `unpack_event_page` /
//! `pack_datum_page` / `unpack_datum_page` / `merge_event_pages` /
//! `rechunk_event_pages` / `merge_datum_pages` / `rechunk_datum_pages`
//! (`__init__.py:2620-2920`).
//! Packing transposes a list of row documents into one column-store page;
//! unpacking reverses it; merging concatenates several pages into one;
//! rechunking re-batches pages to a uniform size. As in the reference, only
//! keys present on a given row contribute to that row's columns, and the first
//! document's descriptor / resource UID is used for the whole page.

use crate::event_model::documents::{Datum, DatumPage, Event, EventPage};
use crate::event_model::EventModelError;
use serde_json::Value;
use std::collections::HashMap;

/// Transpose a slice of `Event`s into a single `EventPage`.
///
/// All events are assumed to share a descriptor (the first event's descriptor
/// UID labels the page). Errors with [`EventModelError::EmptyPack`] if `events`
/// is empty: the page's `descriptor` is taken from the first event and cannot
/// be null (mirrors the reference `pack_event_page` `ValueError`).
pub fn pack_event_page(events: &[Event]) -> Result<EventPage, EventModelError> {
    let descriptor =
        events
            .first()
            .map(|e| e.descriptor.clone())
            .ok_or(EventModelError::EmptyPack {
                kind: "Event",
                field: "descriptor",
            })?;
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
    Ok(EventPage {
        uid,
        descriptor,
        time,
        seq_num,
        data,
        timestamps,
        filled,
    })
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
/// labels the page). Errors with [`EventModelError::EmptyPack`] if `datums` is
/// empty: the page's `resource` is taken from the first datum and cannot be
/// null (mirrors the reference `pack_datum_page` `ValueError`).
pub fn pack_datum_page(datums: &[Datum]) -> Result<DatumPage, EventModelError> {
    let resource =
        datums
            .first()
            .map(|d| d.resource.clone())
            .ok_or(EventModelError::EmptyPack {
                kind: "Datum",
                field: "resource",
            })?;
    let mut datum_id = Vec::with_capacity(datums.len());
    let mut datum_kwargs: HashMap<String, Vec<Value>> = HashMap::new();
    for d in datums {
        datum_id.push(d.datum_id.clone());
        for (k, v) in &d.datum_kwargs {
            datum_kwargs.entry(k.clone()).or_default().push(v.clone());
        }
    }
    Ok(DatumPage {
        datum_id,
        resource,
        datum_kwargs,
    })
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

/// Slice every column of a column-store between `[start, stop)`, clamping to
/// each column's length so a ragged column cannot panic.
fn slice_cols<T: Clone>(
    cols: &HashMap<String, Vec<T>>,
    start: usize,
    stop: usize,
) -> HashMap<String, Vec<T>> {
    cols.iter()
        .map(|(k, v)| {
            let lo = start.min(v.len());
            let hi = stop.min(v.len());
            (k.clone(), v[lo..hi].to_vec())
        })
        .collect()
}

/// Re-batch a slice of `EventPage`s into pages of exactly `chunk_size` rows
/// (the final page holds the remainder).
///
/// Mirrors `event_model.rechunk_event_pages`. For a well-formed page stream
/// (all pages share a descriptor) this is "concatenate every row in order,
/// then re-split into `chunk_size`-row pages" — equivalent to the reference's
/// streaming merge, built here on [`merge_event_pages`]. The merged
/// descriptor labels every output page. Returns an empty `Vec` for empty
/// input or `chunk_size == 0` (which cannot define a chunking).
pub fn rechunk_event_pages(pages: &[EventPage], chunk_size: usize) -> Vec<EventPage> {
    if chunk_size == 0 {
        return Vec::new();
    }
    let merged = merge_event_pages(pages);
    let n = merged.uid.len();
    let mut out = Vec::with_capacity(n.div_ceil(chunk_size));
    let mut start = 0;
    while start < n {
        let stop = (start + chunk_size).min(n);
        out.push(EventPage {
            uid: merged.uid[start..stop].to_vec(),
            descriptor: merged.descriptor.clone(),
            time: merged.time[start..stop].to_vec(),
            seq_num: merged.seq_num[start..stop].to_vec(),
            data: slice_cols(&merged.data, start, stop),
            timestamps: slice_cols(&merged.timestamps, start, stop),
            filled: slice_cols(&merged.filled, start, stop),
        });
        start = stop;
    }
    out
}

/// Combine a slice of `DatumPage`s into one, concatenating every column.
///
/// Mirrors `event_model.merge_datum_pages`. The first page's `resource` UID
/// labels the result. `datum_kwargs` keys are unioned across all pages (like
/// [`pack_datum_page`], rather than the reference's first-page-only key set)
/// and each page's values are appended in order; a page missing a key
/// contributes no rows to that column. Returns an empty page if `pages` is
/// empty, and the sole page (cloned) when there is exactly one.
pub fn merge_datum_pages(pages: &[DatumPage]) -> DatumPage {
    if pages.len() == 1 {
        return pages[0].clone();
    }
    let resource = pages
        .first()
        .map(|p| p.resource.clone())
        .unwrap_or_default();
    let total: usize = pages.iter().map(|p| p.datum_id.len()).sum();
    let mut datum_id = Vec::with_capacity(total);
    let mut datum_kwargs: HashMap<String, Vec<Value>> = HashMap::new();
    for p in pages {
        datum_id.extend(p.datum_id.iter().cloned());
        for (k, col) in &p.datum_kwargs {
            datum_kwargs
                .entry(k.clone())
                .or_default()
                .extend(col.iter().cloned());
        }
    }
    DatumPage {
        datum_id,
        resource,
        datum_kwargs,
    }
}

/// Re-batch a slice of `DatumPage`s into pages of exactly `chunk_size` rows
/// (the final page holds the remainder).
///
/// Mirrors `event_model.rechunk_datum_pages`. For a well-formed page stream
/// (all pages share a resource) this is "concatenate every datum in order,
/// then re-split into `chunk_size`-row pages" — equivalent to the reference's
/// streaming merge, built here on [`merge_datum_pages`]. The merged `resource`
/// labels every output page. Returns an empty `Vec` for empty input or
/// `chunk_size == 0` (which cannot define a chunking).
pub fn rechunk_datum_pages(pages: &[DatumPage], chunk_size: usize) -> Vec<DatumPage> {
    if chunk_size == 0 {
        return Vec::new();
    }
    let merged = merge_datum_pages(pages);
    let n = merged.datum_id.len();
    let mut out = Vec::with_capacity(n.div_ceil(chunk_size));
    let mut start = 0;
    while start < n {
        let stop = (start + chunk_size).min(n);
        out.push(DatumPage {
            datum_id: merged.datum_id[start..stop].to_vec(),
            resource: merged.resource.clone(),
            datum_kwargs: slice_cols(&merged.datum_kwargs, start, stop),
        });
        start = stop;
    }
    out
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
        let page = pack_event_page(&events).unwrap();
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
        let page = pack_datum_page(&datums).unwrap();
        assert_eq!(page.datum_id, vec!["r/1", "r/2"]);
        assert_eq!(page.resource, "r");
        let back = unpack_datum_page(&page);
        assert_eq!(back, datums);
    }

    #[test]
    fn pack_empty_event_page_is_rejected() {
        // The reference raises ValueError: an EventPage's `descriptor` (taken
        // from the first event) cannot be null, so zero events cannot pack.
        let err = pack_event_page(&[]).unwrap_err();
        assert!(
            matches!(
                err,
                EventModelError::EmptyPack {
                    kind: "Event",
                    field: "descriptor"
                }
            ),
            "empty pack must error, got {err:?}"
        );
    }

    #[test]
    fn pack_empty_datum_page_is_rejected() {
        // The reference raises ValueError: a DatumPage's `resource` (taken from
        // the first datum) cannot be null, so zero datums cannot pack.
        let err = pack_datum_page(&[]).unwrap_err();
        assert!(
            matches!(
                err,
                EventModelError::EmptyPack {
                    kind: "Datum",
                    field: "resource"
                }
            ),
            "empty pack must error, got {err:?}"
        );
    }

    #[test]
    fn merge_event_pages_concatenates_columns_and_keeps_first_descriptor() {
        let p1 = pack_event_page(&[event("e-1", 1, 1.5), event("e-2", 2, 2.5)]).unwrap();
        let mut p2 = pack_event_page(&[event("e-3", 3, 3.5)]).unwrap();
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
        let p1 = pack_event_page(&[event("e-1", 1, 1.5)]).unwrap();
        assert_eq!(merge_event_pages(std::slice::from_ref(&p1)), p1);
    }

    #[test]
    fn merge_event_pages_empty_is_safe() {
        let merged = merge_event_pages(&[]);
        assert!(merged.uid.is_empty());
        assert_eq!(merged.descriptor, "");
        assert!(unpack_event_page(&merged).is_empty());
    }

    #[test]
    fn rechunk_event_pages_splits_uniformly_across_page_boundaries() {
        // 3 + 2 = 5 rows, rechunk to 2 → pages of [2, 2, 1]; the middle chunk
        // straddles the input page boundary (e-3 from page A, e-4 from page B).
        let a = pack_event_page(&[
            event("e-1", 1, 1.5),
            event("e-2", 2, 2.5),
            event("e-3", 3, 3.5),
        ])
        .unwrap();
        let b = pack_event_page(&[event("e-4", 4, 4.5), event("e-5", 5, 5.5)]).unwrap();
        let chunks = rechunk_event_pages(&[a, b], 2);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].seq_num, vec![1, 2]);
        assert_eq!(
            chunks[1].seq_num,
            vec![3, 4],
            "chunk straddles the page boundary"
        );
        assert_eq!(chunks[2].seq_num, vec![5]);
        assert_eq!(chunks[1].uid, vec!["e-3", "e-4"]);
        assert_eq!(chunks[1].data["x"], vec![json!(3.5), json!(4.5)]);
        for c in &chunks {
            assert_eq!(c.descriptor, "d-1");
        }
        // Rows survive the round-trip in order.
        let all: Vec<Event> = chunks.iter().flat_map(unpack_event_page).collect();
        assert_eq!(
            all,
            vec![
                event("e-1", 1, 1.5),
                event("e-2", 2, 2.5),
                event("e-3", 3, 3.5),
                event("e-4", 4, 4.5),
                event("e-5", 5, 5.5),
            ]
        );
    }

    #[test]
    fn rechunk_event_pages_chunk_larger_than_total_is_one_page() {
        let p = pack_event_page(&[event("e-1", 1, 1.5), event("e-2", 2, 2.5)]).unwrap();
        let chunks = rechunk_event_pages(&[p], 10);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].seq_num, vec![1, 2]);
    }

    #[test]
    fn rechunk_event_pages_zero_chunk_and_empty_input_yield_nothing() {
        let p = pack_event_page(&[event("e-1", 1, 1.5)]).unwrap();
        assert!(
            rechunk_event_pages(std::slice::from_ref(&p), 0).is_empty(),
            "chunk_size 0 cannot define a chunking"
        );
        assert!(
            rechunk_event_pages(&[], 2).is_empty(),
            "no input → no pages"
        );
    }

    fn datum(id: &str, i: i64) -> Datum {
        Datum {
            datum_id: id.into(),
            resource: "r".into(),
            datum_kwargs: HashMap::from([("i".into(), json!(i))]),
        }
    }

    #[test]
    fn merge_datum_pages_concatenates_columns_and_keeps_first_resource() {
        let p1 = pack_datum_page(&[datum("r/1", 1), datum("r/2", 2)]).unwrap();
        let mut p2 = pack_datum_page(&[datum("r/3", 3)]).unwrap();
        // A differing resource on a later page must not win.
        p2.resource = "r2".into();
        let merged = merge_datum_pages(&[p1, p2]);
        assert_eq!(merged.datum_id, vec!["r/1", "r/2", "r/3"]);
        assert_eq!(
            merged.datum_kwargs["i"],
            vec![json!(1), json!(2), json!(3)],
            "datum_kwargs column concatenated in page order"
        );
        assert_eq!(
            merged.resource, "r",
            "first page's resource labels the merge"
        );
        // The merged page unpacks back to the three original datums.
        let back = unpack_datum_page(&merged);
        assert_eq!(
            back,
            vec![datum("r/1", 1), datum("r/2", 2), datum("r/3", 3)]
        );
    }

    #[test]
    fn merge_datum_pages_single_returns_that_page() {
        let p1 = pack_datum_page(&[datum("r/1", 1)]).unwrap();
        assert_eq!(merge_datum_pages(std::slice::from_ref(&p1)), p1);
    }

    #[test]
    fn merge_datum_pages_empty_is_safe() {
        let merged = merge_datum_pages(&[]);
        assert!(merged.datum_id.is_empty());
        assert_eq!(merged.resource, "");
        assert!(unpack_datum_page(&merged).is_empty());
    }

    #[test]
    fn rechunk_datum_pages_splits_uniformly_across_page_boundaries() {
        // 3 + 2 = 5 datums, rechunk to 2 → pages of [2, 2, 1]; the middle chunk
        // straddles the input page boundary (r/3 from page A, r/4 from page B).
        let a = pack_datum_page(&[datum("r/1", 1), datum("r/2", 2), datum("r/3", 3)]).unwrap();
        let b = pack_datum_page(&[datum("r/4", 4), datum("r/5", 5)]).unwrap();
        let chunks = rechunk_datum_pages(&[a, b], 2);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].datum_id, vec!["r/1", "r/2"]);
        assert_eq!(
            chunks[1].datum_id,
            vec!["r/3", "r/4"],
            "chunk straddles the page boundary"
        );
        assert_eq!(chunks[1].datum_kwargs["i"], vec![json!(3), json!(4)]);
        assert_eq!(chunks[2].datum_id, vec!["r/5"]);
        for c in &chunks {
            assert_eq!(c.resource, "r");
        }
        // Datums survive the round-trip in order.
        let all: Vec<Datum> = chunks.iter().flat_map(unpack_datum_page).collect();
        assert_eq!(
            all,
            vec![
                datum("r/1", 1),
                datum("r/2", 2),
                datum("r/3", 3),
                datum("r/4", 4),
                datum("r/5", 5),
            ]
        );
    }

    #[test]
    fn rechunk_datum_pages_chunk_larger_than_total_is_one_page() {
        let p = pack_datum_page(&[datum("r/1", 1), datum("r/2", 2)]).unwrap();
        let chunks = rechunk_datum_pages(&[p], 10);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].datum_id, vec!["r/1", "r/2"]);
    }

    #[test]
    fn rechunk_datum_pages_zero_chunk_and_empty_input_yield_nothing() {
        let p = pack_datum_page(&[datum("r/1", 1)]).unwrap();
        assert!(
            rechunk_datum_pages(std::slice::from_ref(&p), 0).is_empty(),
            "chunk_size 0 cannot define a chunking"
        );
        assert!(
            rechunk_datum_pages(&[], 2).is_empty(),
            "no input → no pages"
        );
    }
}
