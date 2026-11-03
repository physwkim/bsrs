//! `NdArray`: numpy's `np.ndarray`, an array of any shape whose element type
//! is fixed only by its value, as an NTNDArray PV carries one.

use crate::core::error::{BsrsError, Result};
use serde::ser::{Serialize, SerializeSeq, Serializer};

/// An [`NdArray`]'s elements, flat in C order, as one of numpy's
/// numeric or bool dtypes.
#[derive(Clone, Debug, PartialEq)]
pub enum NdData {
    /// `np.bool_`.
    Bool(Vec<bool>),
    /// `np.int8`.
    I8(Vec<i8>),
    /// `np.uint8`.
    U8(Vec<u8>),
    /// `np.int16`.
    I16(Vec<i16>),
    /// `np.uint16`.
    U16(Vec<u16>),
    /// `np.int32`.
    I32(Vec<i32>),
    /// `np.uint32`.
    U32(Vec<u32>),
    /// `np.int64`.
    I64(Vec<i64>),
    /// `np.uint64`.
    U64(Vec<u64>),
    /// `np.float32`.
    F32(Vec<f32>),
    /// `np.float64`.
    F64(Vec<f64>),
}

/// `$body` with `$v` bound to the `Vec` of whichever dtype `$data` is.
macro_rules! each_dtype {
    ($data:expr, $v:ident => $body:expr) => {
        match $data {
            NdData::Bool($v) => $body,
            NdData::I8($v) => $body,
            NdData::U8($v) => $body,
            NdData::I16($v) => $body,
            NdData::U16($v) => $body,
            NdData::I32($v) => $body,
            NdData::U32($v) => $body,
            NdData::I64($v) => $body,
            NdData::U64($v) => $body,
            NdData::F32($v) => $body,
            NdData::F64($v) => $body,
        }
    };
}

impl NdData {
    /// The number of elements.
    pub fn len(&self) -> usize {
        each_dtype!(self, v => v.len())
    }

    /// Whether there are no elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// numpy's `dtype.str` of the elements, as a datakey's `dtype_numpy`.
    pub fn dtype_numpy(&self) -> &'static str {
        match self {
            Self::Bool(_) => "|b1",
            Self::I8(_) => "|i1",
            Self::U8(_) => "|u1",
            Self::I16(_) => "<i2",
            Self::U16(_) => "<u2",
            Self::I32(_) => "<i4",
            Self::U32(_) => "<u4",
            Self::I64(_) => "<i8",
            Self::U64(_) => "<u8",
            Self::F32(_) => "<f4",
            Self::F64(_) => "<f8",
        }
    }
}

/// The elements as one flat list.
impl Serialize for NdData {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        each_dtype!(self, v => v.serialize(s))
    }
}

macro_rules! nd_data_from {
    ($($t:ty => $v:ident),*) => {$(
        impl From<Vec<$t>> for NdData {
            fn from(v: Vec<$t>) -> Self {
                Self::$v(v)
            }
        }

        /// A 1-D array of `v`.
        impl From<Vec<$t>> for NdArray {
            fn from(v: Vec<$t>) -> Self {
                Self {
                    shape: vec![v.len()],
                    data: NdData::$v(v),
                }
            }
        }
    )*};
}

nd_data_from!(
    bool => Bool, i8 => I8, u8 => U8, i16 => I16, u16 => U16, i32 => I32,
    u32 => U32, i64 => I64, u64 => U64, f32 => F32, f64 => F64
);

/// numpy's `np.ndarray`: elements of one dtype, laid out in C order (the
/// last index changing fastest) over `shape`.
#[derive(Clone, Debug, PartialEq)]
pub struct NdArray {
    shape: Vec<usize>,
    data: NdData,
}

impl NdArray {
    /// `data` reshaped to `shape`, refused with numpy's `ValueError` when
    /// the element counts differ.
    pub fn new(shape: Vec<usize>, data: impl Into<NdData>) -> Result<Self> {
        let data = data.into();
        if shape.iter().product::<usize>() != data.len() {
            let dims: Vec<String> = shape.iter().map(usize::to_string).collect();
            let comma = if shape.len() == 1 { "," } else { "" };
            return Err(BsrsError::InvalidValue(format!(
                "cannot reshape array of size {} into shape ({}{comma})",
                data.len(),
                dims.join(",")
            )));
        }
        Ok(Self { shape, data })
    }

    /// The length of each dimension, outermost first.
    pub fn shape(&self) -> &[usize] {
        &self.shape
    }

    /// The elements, flat in C order.
    pub fn data(&self) -> &NdData {
        &self.data
    }
}

/// ophyd-async's soft `np.ndarray` default: empty, of float64.
impl Default for NdArray {
    fn default() -> Self {
        Self {
            shape: vec![0],
            data: NdData::F64(vec![]),
        }
    }
}

/// `data` as lists nested `shape.len()` deep, as numpy's `tolist()`.
struct Nested<'a, E> {
    shape: &'a [usize],
    data: &'a [E],
}

impl<E: Serialize> Serialize for Nested<'_, E> {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let Some((&n, inner)) = self.shape.split_first() else {
            return self.data[0].serialize(s);
        };
        let stride: usize = inner.iter().product();
        let mut seq = s.serialize_seq(Some(n))?;
        for i in 0..n {
            seq.serialize_element(&Nested {
                shape: inner,
                data: &self.data[i * stride..(i + 1) * stride],
            })?;
        }
        seq.end()
    }
}

impl Serialize for NdArray {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        each_dtype!(&self.data, v => Nested { shape: &self.shape, data: v }.serialize(s))
    }
}

impl From<NdArray> for serde_json::Value {
    fn from(a: NdArray) -> Self {
        serde_json::to_value(&a).expect("numbers and bools serialize to JSON")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serializes_as_nested_lists_in_c_order() {
        let a = NdArray::new(vec![2, 3], (0..6_i64).collect::<Vec<_>>()).unwrap();
        assert_eq!(serde_json::Value::from(a), json!([[0, 1, 2], [3, 4, 5]]));
        let a = NdArray::new(vec![], vec![2.5_f64]).unwrap();
        assert_eq!(serde_json::Value::from(a), json!(2.5));
        let a = NdArray::new(vec![2, 0], Vec::<u8>::new()).unwrap();
        assert_eq!(serde_json::Value::from(a), json!([[], []]));
        assert_eq!(serde_json::Value::from(NdArray::default()), json!([]));
    }

    #[test]
    fn refuses_a_shape_of_another_size_as_numpy() {
        let refusal = |shape: Vec<usize>| match NdArray::new(shape, vec![0_i32; 5]) {
            Err(BsrsError::InvalidValue(why)) => why,
            other => panic!("not refused: {other:?}"),
        };
        assert_eq!(
            refusal(vec![2, 3]),
            "cannot reshape array of size 5 into shape (2,3)"
        );
        assert_eq!(
            refusal(vec![4]),
            "cannot reshape array of size 5 into shape (4,)"
        );
        assert_eq!(
            refusal(vec![]),
            "cannot reshape array of size 5 into shape ()"
        );
    }

    #[test]
    fn a_vec_is_one_dimensional_and_keeps_its_dtype() {
        let a = NdArray::from(vec![1_u16, 2, 3]);
        assert_eq!(a.shape(), [3]);
        assert_eq!(a.data(), &NdData::U16(vec![1, 2, 3]));
        assert_eq!(a.data().dtype_numpy(), "<u2");
        assert_eq!(NdArray::default().data().dtype_numpy(), "<f8");
    }
}
