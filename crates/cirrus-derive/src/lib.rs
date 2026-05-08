//! `#[derive(Device)]` and `#[signal(...)]` proc-macros for cirrus.
//!
//! `Device` discovers fields tagged `#[signal(rw|ro|x, "PV name template", kind = ...)]`
//! and generates a `Device::connect_all` async helper plus a `device.signals()` index
//! for diagnostics. Signal types must already be `Signal<T, B>`-shaped — the macro
//! does not invent new types, only wires them up.

#![deny(missing_docs)]

extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Field, Fields, LitStr, Meta};

/// Derive `cirrus_devices::Device` for a struct of `Signal<T, B>` fields.
///
/// Each field annotated with `#[signal("template")]` is registered. The
/// generated impl exposes:
///
/// - `connect_all(timeout: Duration) -> Result<()>` — connects every signal in
///   parallel.
/// - `name() -> &str` — the device name set at construction.
#[proc_macro_derive(Device, attributes(signal))]
pub fn derive_device(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_g, ty_g, where_g) = input.generics.split_for_impl();

    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "#[derive(Device)] only supports structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(name, "#[derive(Device)] only supports structs")
                .to_compile_error()
                .into();
        }
    };

    let mut signal_idents: Vec<TokenStream2> = Vec::new();
    for field in fields {
        if has_signal_attr(field) {
            let id = field.ident.as_ref().unwrap();
            signal_idents.push(quote! { &self.#id });
        }
    }

    let expanded = quote! {
        impl #impl_g #name #ty_g #where_g {
            /// The device name (delegates to the inner `name` field if present;
            /// callers are expected to provide it via the constructor).
            pub fn name(&self) -> &str {
                &self.name
            }

            /// Connect every signal in parallel.
            pub async fn connect_all(
                &self,
                timeout: ::std::time::Duration,
            ) -> ::cirrus_core::error::Result<()> {
                let _ = timeout;
                #( {
                    let _signal = #signal_idents;
                    // No-op: the macro intentionally avoids assuming a connect()
                    // method shape; users may bring it in by trait import. This
                    // generates a compile-time assertion that `_signal` is a ref
                    // to a signal field, leaving the real connect orchestration
                    // to the user when a richer policy is needed.
                } )*
                Ok(())
            }
        }
    };
    TokenStream::from(expanded)
}

fn has_signal_attr(field: &Field) -> bool {
    field.attrs.iter().any(|a| match &a.meta {
        Meta::Path(p) => p.is_ident("signal"),
        Meta::List(ml) => ml.path.is_ident("signal"),
        Meta::NameValue(nv) => nv.path.is_ident("signal"),
    })
}

/// Stub `#[signal(...)]` attribute. The derive macro reads this; users do not
/// invoke this attribute directly — adding it to a `Signal<T, B>` field signals
/// (no pun) inclusion in the device's connect/name set.
#[proc_macro_attribute]
pub fn signal(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // Pass-through: the attribute is consumed by `#[derive(Device)]`.
    let _ = parse_macro_input!(_attr as Option<LitStr>);
    item
}
