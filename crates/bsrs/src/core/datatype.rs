//! `SignalDatatype`: a value type a soft signal holds, described as
//! ophyd-async's `make_datakey` describes it from the value
//! (`core/_signal_backend.py:128-195`).

use crate::core::error::Result;
use crate::core::ndarray::NdArray;
use crate::core::table::TypedTable;
use crate::event_model::{make_datakey, DataKey, Dtype, SignalMetadata};

/// A value a soft or mock signal holds, and the [`DataKey`] ophyd-async's
/// `make_datakey` gives it: its `dtype`, the value's shape, and the numpy
/// dtype of the value or its elements.
pub trait SignalDatatype {
    /// `make_datakey(datatype, self, source, metadata)`.
    fn make_datakey(&self, source: String, meta: SignalMetadata) -> Result<DataKey>;
}

macro_rules! primitive {
    ($($t:ty => $dtype:ident, $np:literal;)*) => {$(
        impl SignalDatatype for $t {
            fn make_datakey(&self, source: String, meta: SignalMetadata) -> Result<DataKey> {
                Ok(make_datakey(source, Dtype::$dtype, vec![], Some($np.into()), meta))
            }
        }
    )*};
}

primitive! {
    bool => Boolean, "|b1";
    i64 => Integer, "<i8";
    f64 => Number, "<f8";
    String => String, "|S40";
    // A `SignalX`'s, whose ophyd-async datatype `None` is described as float.
    () => Number, "<f8";
}

/// `Array1D` of the element's dtype, or for `String` a `Sequence[str]`.
macro_rules! array {
    ($($t:ty => $np:literal;)*) => {$(
        impl SignalDatatype for Vec<$t> {
            fn make_datakey(&self, source: String, meta: SignalMetadata) -> Result<DataKey> {
                let shape = vec![Some(self.len() as u64)];
                Ok(make_datakey(source, Dtype::Array, shape, Some($np.into()), meta))
            }
        }
    )*};
}

array! {
    bool => "|b1";
    i8 => "|i1";
    u8 => "|u1";
    i16 => "<i2";
    u16 => "<u2";
    i32 => "<i4";
    u32 => "<u4";
    i64 => "<i8";
    u64 => "<u8";
    f32 => "<f4";
    f64 => "<f8";
    String => "|S40";
}

impl SignalDatatype for NdArray {
    fn make_datakey(&self, source: String, meta: SignalMetadata) -> Result<DataKey> {
        let shape = self.shape().iter().map(|&n| Some(n as u64)).collect();
        let np = self.data().dtype_numpy();
        Ok(make_datakey(
            source,
            Dtype::Array,
            shape,
            Some(np.into()),
            meta,
        ))
    }
}

/// Refused when the columns' lengths differ, as such a table is.
impl<T: TypedTable> SignalDatatype for T {
    fn make_datakey(&self, source: String, meta: SignalMetadata) -> Result<DataKey> {
        let table = self.clone().into_table()?;
        let shape = vec![Some(table.len() as u64)];
        let np = table.dtype_numpy();
        Ok(make_datakey(source, Dtype::Array, shape, Some(np), meta))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_model::DtypeNumpy;

    fn described<T: SignalDatatype>(v: T) -> (Dtype, Vec<Option<u64>>, Option<DtypeNumpy>) {
        let dk = v
            .make_datakey("soft://test".into(), SignalMetadata::default())
            .unwrap();
        (dk.dtype, dk.shape, dk.dtype_numpy)
    }

    #[test]
    fn an_array_is_shaped_as_its_value() {
        let np = |s: &str| Some(DtypeNumpy::from(s));
        assert_eq!(
            described(Vec::<f64>::new()),
            (Dtype::Array, vec![Some(0)], np("<f8"))
        );
        assert_eq!(
            described(vec!["a".to_string(), "b".into()]),
            (Dtype::Array, vec![Some(2)], np("|S40"))
        );
        let image = NdArray::new(vec![2, 3], vec![0_u16; 6]).unwrap();
        assert_eq!(
            described(image),
            (Dtype::Array, vec![Some(2), Some(3)], np("<u2"))
        );
    }

    #[test]
    fn a_table_of_unequal_columns_is_refused() {
        #[derive(Clone, Debug, PartialEq, crate::core::Table)]
        struct Unequal {
            a: Vec<i32>,
            b: Vec<f64>,
        }
        let t = Unequal {
            a: vec![1, 2],
            b: vec![1.0],
        };
        let err = t
            .make_datakey("soft://test".into(), SignalMetadata::default())
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid value: Columns should be same length, got lengths={2: {'a'}, 1: {'b'}}"
        );
    }
}
