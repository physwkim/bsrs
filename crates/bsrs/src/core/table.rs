//! `Table`: ophyd-async's `Table`, named columns of one length, each an
//! `Array1D` of one numpy dtype or a `Sequence[str]`, as an NTTable PV
//! carries one. A struct of such columns is a typed table, ophyd-async's
//! `Table` subclass, through `#[derive(Table)]`.

use crate::core::error::{BsrsError, Result};
use crate::core::ndarray::NdData;
use crate::devices::signal::shown;
use crate::event_model::DtypeNumpy;
use serde::ser::{Serialize, SerializeMap, Serializer};

/// A [`Table`] column.
#[derive(Clone, Debug, PartialEq)]
pub enum Column {
    /// `Array1D` of the elements' dtype.
    Array(NdData),
    /// `Sequence[str]`, which an enum column is too.
    Str(Vec<String>),
}

impl Column {
    /// The number of rows.
    pub fn len(&self) -> usize {
        match self {
            Self::Array(d) => d.len(),
            Self::Str(v) => v.len(),
        }
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// numpy's `dtype.str` of the column, a string one as `S40`.
    pub fn dtype_numpy(&self) -> &'static str {
        match self {
            Self::Array(d) => d.dtype_numpy(),
            Self::Str(_) => "|S40",
        }
    }
}

impl<E: TableElement> From<Vec<E>> for Column {
    fn from(v: Vec<E>) -> Self {
        E::into_column(v)
    }
}

impl Serialize for Column {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Array(d) => d.serialize(s),
            Self::Str(v) => v.serialize(s),
        }
    }
}

/// ophyd-async's plain `Table`: columns of any dtype, in order, all of one
/// length.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Table {
    columns: Vec<(String, Column)>,
}

impl Table {
    /// `columns` in order, refused as ophyd-async refuses columns of
    /// different lengths, and when two share a name.
    pub fn new<S: Into<String>>(columns: impl IntoIterator<Item = (S, Column)>) -> Result<Self> {
        let columns: Vec<(String, Column)> =
            columns.into_iter().map(|(n, c)| (n.into(), c)).collect();
        for (i, (name, _)) in columns.iter().enumerate() {
            if columns[..i].iter().any(|(n, _)| n == name) {
                return Err(BsrsError::InvalidValue(format!("duplicate column {name}")));
            }
        }
        check_lengths(columns.iter().map(|(n, c)| (n.as_str(), c.len())))?;
        Ok(Self { columns })
    }

    /// The columns, in order.
    pub fn columns(&self) -> &[(String, Column)] {
        &self.columns
    }

    /// The column named `name`.
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|(n, _)| n == name).map(|(_, c)| c)
    }

    /// The columns, in order.
    pub fn into_columns(self) -> Vec<(String, Column)> {
        self.columns
    }

    /// The number of rows.
    pub fn len(&self) -> usize {
        self.columns.first().map_or(0, |(_, c)| c.len())
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// A datakey's `dtype_numpy`: numpy's structured `descr` of the
    /// columns, or as ophyd-async's `make_datakey` gives one of fewer than
    /// two columns, its `dtype.str`, a void of the row's size.
    pub fn dtype_numpy(&self) -> DtypeNumpy {
        let descr: Vec<(String, String)> = self
            .columns
            .iter()
            .map(|(n, c)| (n.clone(), c.dtype_numpy().to_owned()))
            .collect();
        if descr.len() > 1 {
            return DtypeNumpy::Structured(descr);
        }
        // Every dtype string is a byte-order char, a kind char and a size.
        let size: usize = descr
            .iter()
            .filter_map(|(_, d)| d[2..].parse::<usize>().ok())
            .sum();
        DtypeNumpy::Scalar(format!("|V{size}"))
    }
}

/// The columns as a map of name to list, as pydantic's `model_dump`.
impl Serialize for Table {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(self.columns.len()))?;
        for (name, column) in &self.columns {
            m.serialize_entry(name, column)?;
        }
        m.end()
    }
}

impl From<Table> for serde_json::Value {
    fn from(t: Table) -> Self {
        serde_json::to_value(&t).expect("columns serialize to JSON")
    }
}

/// ophyd-async's `_validate_lengths`.
fn check_lengths<'a>(columns: impl IntoIterator<Item = (&'a str, usize)>) -> Result<()> {
    let mut lengths: Vec<(usize, Vec<&str>)> = Vec::new();
    for (name, len) in columns {
        match lengths.iter_mut().find(|(l, _)| *l == len) {
            Some((_, names)) => names.push(name),
            None => lengths.push((len, vec![name])),
        }
    }
    if lengths.len() < 2 {
        return Ok(());
    }
    let groups: Vec<String> = lengths
        .iter()
        .map(|(len, names)| {
            let names: Vec<String> = names.iter().map(|n| format!("'{n}'")).collect();
            format!("{len}: {{{}}}", names.join(", "))
        })
        .collect();
    Err(BsrsError::InvalidValue(format!(
        "Columns should be same length, got lengths={{{}}}",
        groups.join(", ")
    )))
}

/// ophyd-async's `Table`, or a subclass of it: a struct whose fields are
/// its columns, each a `Vec` of a [`TableElement`]. `#[derive(Table)]`
/// implements it, with `Serialize` and `Into<serde_json::Value>` as its
/// columns' map.
pub trait TypedTable: Clone + Send + Sync + Serialize + Into<serde_json::Value> + 'static {
    /// The struct's name, as a refusal names the datatype.
    const NAME: &'static str;

    /// The fields as a [`Table`], refused when their lengths differ.
    fn into_table(self) -> Result<Table>;

    /// `table` as this struct, as ophyd-async validates a subclass: each
    /// column cast to its field's dtype if no value changes, a missing one
    /// empty, any other column refused, and the lengths checked.
    fn from_table(table: Table) -> Result<Self>;
}

/// The plain `Table` takes any columns.
impl TypedTable for Table {
    const NAME: &'static str = "Table";
    fn into_table(self) -> Result<Table> {
        Ok(self)
    }
    fn from_table(table: Table) -> Result<Self> {
        Ok(table)
    }
}

mod sealed {
    pub trait Sealed {}
}

/// The element of a [`TypedTable`] column: `bool`, `i8`, `u8`, `i16`,
/// `u16`, `i32`, `u32`, `i64`, `u64`, `f32`, `f64` for an `Array1D`, or
/// `String` for a `Sequence[str]` or an enum.
pub trait TableElement: Sized + sealed::Sealed {
    #[doc(hidden)]
    fn into_column(v: Vec<Self>) -> Column;
    /// The column `name` cast to this element, as ophyd-async's
    /// `_validate_array_dtypes`.
    #[doc(hidden)]
    fn from_column(name: &str, c: Column) -> Result<Vec<Self>>;
}

impl sealed::Sealed for String {}

impl TableElement for String {
    fn into_column(v: Vec<Self>) -> Column {
        Column::Str(v)
    }
    fn from_column(name: &str, c: Column) -> Result<Vec<Self>> {
        match c {
            Column::Str(v) => Ok(v),
            Column::Array(_) => Err(BsrsError::InvalidValue(format!(
                "{name}: Input should be a valid string"
            ))),
        }
    }
}

/// An element's value, as numpy compares two arrays of different dtypes.
#[derive(Clone, Copy)]
enum Num {
    Int(i128),
    Float(f64),
}

fn nums(d: &NdData) -> Vec<Num> {
    fn ints<T: Copy + Into<i128>>(v: &[T]) -> Vec<Num> {
        v.iter().map(|&x| Num::Int(x.into())).collect()
    }
    match d {
        NdData::Bool(v) => ints(v),
        NdData::I8(v) => ints(v),
        NdData::U8(v) => ints(v),
        NdData::I16(v) => ints(v),
        NdData::U16(v) => ints(v),
        NdData::I32(v) => ints(v),
        NdData::U32(v) => ints(v),
        NdData::I64(v) => ints(v),
        NdData::U64(v) => ints(v),
        NdData::F32(v) => v.iter().map(|&x| Num::Float(x.into())).collect(),
        NdData::F64(v) => v.iter().map(|&x| Num::Float(x)).collect(),
    }
}

/// A numeric element: the cast of a [`Num`] that `np.array_equal` finds
/// equal to it, so a NaN casts to nothing.
trait Exact: Sized {
    const DTYPE: &'static str;
    fn exact(n: Num) -> Option<Self>;
}

macro_rules! exact_int {
    ($($t:ty => $dtype:literal),*) => {$(
        impl Exact for $t {
            const DTYPE: &'static str = $dtype;
            fn exact(n: Num) -> Option<Self> {
                let i = match n {
                    Num::Int(i) => i,
                    Num::Float(f) if f.fract() == 0.0 => f as i128,
                    Num::Float(_) => return None,
                };
                Self::try_from(i).ok()
            }
        }
    )*};
}

exact_int!(
    i8 => "int8", u8 => "uint8", i16 => "int16", u16 => "uint16", i32 => "int32",
    u32 => "uint32", i64 => "int64", u64 => "uint64"
);

impl Exact for bool {
    const DTYPE: &'static str = "bool";
    fn exact(n: Num) -> Option<Self> {
        match n {
            Num::Int(0) => Some(false),
            Num::Int(1) => Some(true),
            Num::Float(0.0) => Some(false),
            Num::Float(1.0) => Some(true),
            _ => None,
        }
    }
}

/// numpy compares an int with a float64 as float64, so any int casts.
impl Exact for f64 {
    const DTYPE: &'static str = "float64";
    fn exact(n: Num) -> Option<Self> {
        match n {
            Num::Int(i) => Some(i as f64),
            Num::Float(f) => (!f.is_nan()).then_some(f),
        }
    }
}

impl Exact for f32 {
    const DTYPE: &'static str = "float32";
    fn exact(n: Num) -> Option<Self> {
        let (wide, cast) = match n {
            Num::Int(i) => (i as f64, i as f32),
            Num::Float(f) => (f, f as f32),
        };
        (f64::from(cast) == wide).then_some(cast)
    }
}

macro_rules! numeric_element {
    ($($t:ty),*) => {$(
        impl sealed::Sealed for $t {}

        impl TableElement for $t {
            fn into_column(v: Vec<Self>) -> Column {
                Column::Array(v.into())
            }
            fn from_column(name: &str, c: Column) -> Result<Vec<Self>> {
                let cast = match &c {
                    Column::Array(d) => nums(d).into_iter().map(<$t>::exact).collect(),
                    Column::Str(_) => None,
                };
                cast.ok_or_else(|| {
                    BsrsError::InvalidValue(format!(
                        "{name}: Cannot cast {} to {} without losing precision",
                        shown(&c),
                        <$t>::DTYPE
                    ))
                })
            }
        }
    )*};
}

numeric_element!(bool, i8, u8, i16, u16, i32, u32, i64, u64, f32, f64);

/// What `#[derive(Table)]`'s `from_table` takes its fields from.
#[doc(hidden)]
pub struct TableFields {
    columns: Vec<(String, Column)>,
    lengths: Vec<(&'static str, usize)>,
}

impl From<Table> for TableFields {
    fn from(t: Table) -> Self {
        Self {
            columns: t.columns,
            lengths: Vec::new(),
        }
    }
}

impl TableFields {
    /// The column `name` cast to `E`, or empty if there is none.
    pub fn take<E: TableElement>(&mut self, name: &'static str) -> Result<Vec<E>> {
        let v = match self.columns.iter().position(|(n, _)| n == name) {
            Some(i) => E::from_column(name, self.columns.remove(i).1)?,
            None => Vec::new(),
        };
        self.lengths.push((name, v.len()));
        Ok(v)
    }

    /// Refuse a column no field took, then fields of different lengths.
    pub fn finish(self) -> Result<()> {
        if let Some((name, _)) = self.columns.first() {
            return Err(BsrsError::InvalidValue(format!(
                "{name}: Extra inputs are not permitted"
            )));
        }
        check_lengths(self.lengths)
    }
}

/// Re-exports for the `#[derive(Table)]` expansion.
#[doc(hidden)]
pub mod __macro_support {
    pub use serde;
    pub use serde_json;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Table;
    use serde_json::json;
    use std::fmt::Debug;

    // ophyd-async test_table.py's MyTable.
    #[derive(Clone, Debug, PartialEq, Table)]
    struct MyTable {
        r#bool: Vec<bool>,
        uint: Vec<u32>,
        r#str: Vec<String>,
    }

    fn my_table(columns: [(&str, Column); 3]) -> Result<MyTable> {
        MyTable::from_table(Table::new(columns)?)
    }

    fn refusal<T: Debug>(r: Result<T>) -> String {
        match r {
            Err(BsrsError::InvalidValue(why)) => why,
            other => panic!("not refused: {other:?}"),
        }
    }

    fn strs(v: &[&str]) -> Column {
        Column::from(v.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    // test_table_wrong_types: each column is cast only when no value
    // changes. Its 2-D case has no Column to be.
    #[test]
    fn a_typed_column_refuses_a_cast_that_loses_precision() {
        let cases = [
            (
                [
                    ("bool", Column::from(vec![3_i64, 4])),
                    ("uint", Column::from(vec![3_i64, 4])),
                    ("str", strs(&["", ""])),
                ],
                "bool: Cannot cast [3, 4] to bool without losing precision",
            ),
            (
                [
                    ("bool", Column::from(vec![1_u8])),
                    ("uint", Column::from(vec![-3_i64])),
                    ("str", strs(&[""])),
                ],
                "uint: Cannot cast [-3] to uint32 without losing precision",
            ),
            (
                [
                    ("bool", Column::from(vec![0_i64])),
                    ("uint", Column::from(vec![1.8_f64])),
                    ("str", strs(&[""])),
                ],
                "uint: Cannot cast [1.8] to uint32 without losing precision",
            ),
            (
                [
                    ("bool", Column::from(vec![0_i64, 1])),
                    ("uint", Column::from(vec![3_i64, 4])),
                    ("str", Column::from(vec![44_i64, 0])),
                ],
                "str: Input should be a valid string",
            ),
        ];
        for (columns, why) in cases {
            assert_eq!(refusal(my_table(columns)), why);
        }
    }

    // test_table_coerces
    #[test]
    fn a_typed_column_takes_a_cast_that_keeps_every_value() {
        let t = my_table([
            ("bool", Column::from(vec![1_u8])),
            ("uint", Column::from(vec![3_i64])),
            ("str", strs(&["a"])),
        ])
        .unwrap();
        assert_eq!(
            (t.r#bool, t.uint, t.r#str),
            (vec![true], vec![3], vec!["a".into()])
        );
        let t = my_table([
            ("bool", Column::from(vec![false])),
            ("uint", Column::from(vec![1.0_f64])),
            ("str", strs(&["b"])),
        ])
        .unwrap();
        assert_eq!((t.r#bool, t.uint), (vec![false], vec![1]));
    }

    // test_validate_array_dtypes
    #[test]
    fn an_int32_column_refuses_fractions_and_takes_whole_or_no_values() {
        #[derive(Clone, Debug, PartialEq, Table)]
        struct TestTable {
            int_array: Vec<i32>,
        }
        let cast = |v: Vec<f64>| TestTable::from_table(Table::new([("int_array", v.into())])?);
        assert_eq!(
            refusal(cast(vec![1.5, 2.5])),
            "int_array: Cannot cast [1.5, 2.5] to int32 without losing precision"
        );
        assert_eq!(cast(vec![1.0, 2.0]).unwrap().int_array, vec![1, 2]);
        assert_eq!(cast(vec![]).unwrap().int_array, Vec::<i32>::new());
    }

    // numpy's array_equal finds NaN unequal to itself, so even a float64
    // column refuses one.
    #[test]
    fn a_float_column_refuses_nan_as_numpy_compares_it() {
        #[derive(Clone, Debug, PartialEq, Table)]
        struct Floats {
            f: Vec<f64>,
        }
        let cast = |v: Vec<f64>| Floats::from_table(Table::new([("f", v.into())])?);
        assert_eq!(
            refusal(cast(vec![f64::NAN])),
            "f: Cannot cast [None] to float64 without losing precision"
        );
        assert_eq!(cast(vec![0.5]).unwrap().f, vec![0.5]);
    }

    // A subclass forbids extra columns, defaults a missing one to empty,
    // then checks the lengths.
    #[test]
    fn a_typed_table_refuses_extra_columns_and_unequal_lengths() {
        let extra = Table::new([
            ("bool", Column::from(vec![true])),
            ("uint", Column::from(vec![1_u32])),
            ("str", strs(&["a"])),
            ("more", Column::from(vec![1_u32])),
        ]);
        assert_eq!(
            refusal(MyTable::from_table(extra.unwrap())),
            "more: Extra inputs are not permitted"
        );
        let missing = Table::new([("bool", Column::from(vec![true]))]).unwrap();
        assert_eq!(
            refusal(MyTable::from_table(missing)),
            "Columns should be same length, got lengths={1: {'bool'}, 0: {'uint', 'str'}}"
        );
        let t = MyTable::from_table(Table::default()).unwrap();
        assert!(t.r#bool.is_empty() && t.uint.is_empty() && t.r#str.is_empty());
        let unequal = MyTable {
            r#bool: vec![true; 4],
            uint: vec![1, 2],
            r#str: vec![String::new(); 4],
        };
        assert_eq!(
            refusal(unequal.into_table()),
            "Columns should be same length, got lengths={4: {'bool', 'str'}, 2: {'uint'}}"
        );
    }

    #[test]
    fn a_plain_table_takes_any_columns_of_one_length_once_each() {
        assert_eq!(
            refusal(Table::new([
                ("a", Column::from(vec![1_i32; 4])),
                ("b", Column::from(vec![1_i32; 2])),
            ])),
            "Columns should be same length, got lengths={4: {'a'}, 2: {'b'}}"
        );
        assert_eq!(
            refusal(Table::new([
                ("a", Column::from(vec![1_i32])),
                ("a", strs(&["x"]))
            ])),
            "duplicate column a"
        );
        let t = Table::new([
            ("a", Column::from(vec![1_i32, 2])),
            ("b", strs(&["x", "y"])),
        ]);
        let t = t.unwrap();
        assert_eq!((t.len(), t.column("b")), (2, Some(&strs(&["x", "y"]))));
        assert_eq!(Table::default().len(), 0);
    }

    // ophyd-async's make_datakey: a structured dtype's descr, or for fewer
    // than two columns its str, a void of the row's size.
    #[test]
    fn dtype_numpy_is_the_descr_or_a_void_of_the_row() {
        let t = Table::new([("a", Column::from(vec![true])), ("b", strs(&["x"]))]).unwrap();
        assert_eq!(
            t.dtype_numpy(),
            DtypeNumpy::Structured(vec![
                ("a".into(), "|b1".into()),
                ("b".into(), "|S40".into())
            ])
        );
        let one = |c: Column| Table::new([("a", c)]).unwrap().dtype_numpy();
        assert_eq!(one(Column::from(vec![0_i32])), "|V4".into());
        assert_eq!(one(strs(&[])), "|V40".into());
        assert_eq!(Table::default().dtype_numpy(), "|V0".into());
    }

    #[test]
    fn serializes_as_a_map_of_columns_in_order() {
        let t = MyTable {
            r#bool: vec![true],
            uint: vec![7],
            r#str: vec!["a".into()],
        };
        let json = serde_json::to_string(&t).unwrap();
        assert_eq!(json, r#"{"bool":[true],"uint":[7],"str":["a"]}"#);
        assert_eq!(
            serde_json::Value::from(t.clone().into_table().unwrap()),
            json!({"bool": [true], "uint": [7], "str": ["a"]})
        );
        assert_eq!(
            MyTable::from_table(t.clone().into_table().unwrap()).unwrap(),
            t
        );
    }
}
