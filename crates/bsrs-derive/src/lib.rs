//! `#[derive(Device)]` and helper attributes for bsrs.
//!
//! Generates from a struct definition:
//!
//! - `MyDevice::new(prefix: &str) -> Arc<Self>` — walks `#[signal(...)]`
//!   fields, builds each `Signal<T, B>` with the resolved PV name, and
//!   `#[device("subprefix")]` fields by recursively calling `Sub::new`.
//! - `MyDevice::connect_all(timeout) -> Result<()>` — concurrently connects
//!   every signal and sub-device via `wait_for_connection`.
//! - `MyDevice::name() -> &str` — the prefix passed to `new`.
//!
//! Field attribute syntax:
//!
//! ```text
//! #[signal(rw, "{prefix}.VAL")]                   pub setpoint: Signal<f64, B>,
//! #[signal(ro, "{prefix}.RBV", kind = hinted)]    pub readback: Signal<f64, B>,
//! #[signal(rw, "{prefix}.VELO", kind = config)]   pub velocity: Signal<f64, B>,
//! #[device("{prefix}:x")]                         pub x: Arc<Motor>,
//! ```
//!
//! `#[signal(...)]` and `#[device(...)]` are *helper attributes* of the
//! derive — they have no standalone proc_macro_attribute and are resolved
//! through `#[derive(Device)]`.

#![deny(missing_docs)]

extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, Data, DeriveInput, Field, Fields, Ident, LitStr, Token};

/// Derive `Device` for a struct.
#[proc_macro_derive(Device, attributes(signal, device))]
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

    // Two parallel constructors share one field walk: `new` names each signal
    // by its PV source (backward-compatible); `new_named` propagates the
    // bluesky `{dev_name}-{field}` convention recursively (CP-06).
    let mut field_inits: Vec<TokenStream2> = Vec::new();
    let mut field_inits_named: Vec<TokenStream2> = Vec::new();
    let mut connect_calls: Vec<TokenStream2> = Vec::new();
    // The field name each `connect_calls` entry reports a failure under.
    let mut connect_names: Vec<String> = Vec::new();
    // Per-field `(dotted_path, source)` pushes for `Device::walk_signal_sources`
    // (CP-20). Same field walk as `connect_calls`: a `#[signal]` field pushes
    // its prefixed name + transport source; a `#[device]` field recurses.
    let mut walk_pushes: Vec<TokenStream2> = Vec::new();
    let mut has_name_field = false;

    for field in fields {
        let id = field.ident.as_ref().unwrap();
        let id_str = id.to_string();

        if id == "name" {
            has_name_field = true;
            field_inits.push(quote! {
                name: prefix.to_string(),
            });
            field_inits_named.push(quote! {
                name: dev_name.to_string(),
            });
            continue;
        }

        let signal = match field_attr::<SignalAttr>(field, "signal") {
            Ok(v) => v,
            Err(e) => return e.to_compile_error().into(),
        };
        if let Some(SignalAttr { template, kind }) = signal {
            let kind = format_ident!("{}", kind);
            let kind_expr = quote! { ::bsrs::core::Kind::#kind };
            field_inits.push(quote! {
                #id: ::bsrs::devices::Signal::new(
                    ::std::sync::Arc::new(::bsrs::devices::__derive::default_backend(
                        &::bsrs::devices::__derive::expand(#template, prefix),
                    )),
                    ::bsrs::devices::SignalConfig {
                        source: ::bsrs::devices::__derive::expand(#template, prefix),
                        kind: #kind_expr,
                        name: ::bsrs::devices::__derive::expand(#template, prefix),
                    },
                ),
            });
            field_inits_named.push(quote! {
                #id: ::bsrs::devices::Signal::new(
                    ::std::sync::Arc::new(::bsrs::devices::__derive::default_backend(
                        &::bsrs::devices::__derive::expand(#template, prefix),
                    )),
                    ::bsrs::devices::SignalConfig {
                        source: ::bsrs::devices::__derive::expand(#template, prefix),
                        kind: #kind_expr,
                        name: ::std::format!("{}-{}", dev_name, #id_str),
                    },
                ),
            });
            connect_calls.push(quote! {
                ::bsrs::devices::__derive::connect_signal(&self.#id, timeout)
            });
            connect_names.push(id_str.clone());
            walk_pushes.push(quote! {
                out.push((::std::format!("{}{}", prefix, #id_str), self.#id.source()));
            });
            continue;
        }

        let device = match field_attr::<LitStr>(field, "device") {
            Ok(v) => v,
            Err(e) => return e.to_compile_error().into(),
        };
        if let Some(template) = device {
            let ty = strip_arc(&field.ty);
            field_inits.push(quote! {
                #id: <#ty>::new(&::bsrs::devices::__derive::expand(#template, prefix)),
            });
            field_inits_named.push(quote! {
                #id: <#ty>::new_named(
                    &::bsrs::devices::__derive::expand(#template, prefix),
                    &::std::format!("{}-{}", dev_name, #id_str),
                ),
            });
            connect_calls.push(quote! {
                ::std::boxed::Box::pin(async move { self.#id.connect_all(timeout).await })
            });
            connect_names.push(id_str.clone());
            walk_pushes.push(quote! {
                ::bsrs::devices::Device::walk_signal_sources(
                    &self.#id,
                    &::std::format!("{}{}.", prefix, #id_str),
                    out,
                );
            });
            continue;
        }

        // Unattributed field — must be Default-constructible.
        field_inits.push(quote! {
            #id: ::core::default::Default::default(),
        });
        field_inits_named.push(quote! {
            #id: ::core::default::Default::default(),
        });
    }

    if !has_name_field {
        return syn::Error::new_spanned(
            name,
            "#[derive(Device)] requires a `name: String` field on the struct",
        )
        .to_compile_error()
        .into();
    }

    let connect_block = if connect_calls.is_empty() {
        quote! { Ok(()) }
    } else {
        quote! {
            ::bsrs::devices::wait_for_connection([
                #( (#connect_names, ::std::boxed::Box::pin(#connect_calls)
                    as ::std::pin::Pin<::std::boxed::Box<
                        dyn ::std::future::Future<Output = ::bsrs::core::error::Result<()>>
                            + ::std::marker::Send
                            + '_
                    >>), )*
            ]).await
        }
    };

    let expanded = quote! {
        impl #impl_g #name #ty_g #where_g {
            /// Build the device, allocating each signal under `prefix`. Each
            /// signal is named by its PV source.
            pub fn new(prefix: &str) -> ::std::sync::Arc<Self> {
                ::std::sync::Arc::new(Self { #( #field_inits )* })
            }

            /// Build the device under `prefix`, naming it `dev_name` and
            /// propagating bluesky-style `{dev_name}-{field}` names to every
            /// signal and (recursively) every sub-device (CP-06). This is the
            /// construction-time equivalent of ophyd-async `set_name` — bsrs
            /// devices are immutable `Arc<Self>`, so names are fixed at build
            /// time rather than mutated post-hoc.
            pub fn new_named(prefix: &str, dev_name: &str) -> ::std::sync::Arc<Self> {
                ::std::sync::Arc::new(Self { #( #field_inits_named )* })
            }

            /// Stable device name (`prefix` for [`new`], `dev_name` for
            /// [`new_named`]).
            ///
            /// [`new`]: Self::new
            /// [`new_named`]: Self::new_named
            pub fn name(&self) -> &str {
                &self.name
            }

            /// Connect every signal and sub-device concurrently; a failure
            /// names every field that failed
            /// ([`NotConnected`](::bsrs::core::error::BsrsError::NotConnected)).
            pub async fn connect_all(
                &self,
                timeout: ::std::time::Duration,
            ) -> ::bsrs::core::error::Result<()> {
                #connect_block
            }
        }

        impl #impl_g ::bsrs::devices::Device for #name #ty_g #where_g {
            fn name(&self) -> &str {
                &self.name
            }
            fn connect_all_boxed<'a>(
                &'a self,
                timeout: ::std::time::Duration,
            ) -> ::std::pin::Pin<::std::boxed::Box<
                dyn ::std::future::Future<Output = ::bsrs::core::error::Result<()>>
                    + ::std::marker::Send
                    + 'a,
            >> {
                ::std::boxed::Box::pin(self.connect_all(timeout))
            }
            fn walk_signal_sources(
                &self,
                prefix: &str,
                out: &mut ::std::vec::Vec<(::std::string::String, ::std::string::String)>,
            ) {
                #( #walk_pushes )*
            }
        }
    };
    TokenStream::from(expanded)
}

/// Parse the `#[name(...)]` helper attribute on `field`, if present.
fn field_attr<T: Parse>(field: &Field, name: &str) -> syn::Result<Option<T>> {
    field
        .attrs
        .iter()
        .find(|a| a.path().is_ident(name))
        .map(|a| a.parse_args::<T>())
        .transpose()
}

/// `#[signal([ro|rw|wo|x,] "template" [, kind = normal|config|hinted|omitted])]`.
/// The role is documentation only (the field type carries it); the kind name
/// is case-insensitive like ophyd's `Kind[val.lower()]`.
struct SignalAttr {
    template: LitStr,
    /// `bsrs::core::Kind` variant name.
    kind: &'static str,
}

impl Parse for SignalAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.peek(Ident) {
            let role: Ident = input.parse()?;
            if !matches!(role.to_string().as_str(), "ro" | "rw" | "wo" | "x") {
                return Err(syn::Error::new(
                    role.span(),
                    "expected access role `ro`, `rw`, `wo` or `x`",
                ));
            }
            input.parse::<Token![,]>()?;
        }
        let template: LitStr = input.parse()?;
        let mut kind = "Normal";
        if input.parse::<Option<Token![,]>>()?.is_some() && !input.is_empty() {
            let key: Ident = input.parse()?;
            if key != "kind" {
                return Err(syn::Error::new(key.span(), "expected `kind = ...`"));
            }
            input.parse::<Token![=]>()?;
            let value: Ident = input.parse()?;
            kind = match value.to_string().to_ascii_lowercase().as_str() {
                "normal" => "Normal",
                "config" => "Config",
                "hinted" => "Hinted",
                "omitted" => "Omitted",
                _ => {
                    return Err(syn::Error::new(
                        value.span(),
                        "expected kind `normal`, `config`, `hinted` or `omitted`",
                    ))
                }
            };
            input.parse::<Option<Token![,]>>()?;
        }
        Ok(Self { template, kind })
    }
}

/// If the type is `Arc<T>`, strip to `T` (so `T::new(...)` resolves).
fn strip_arc(ty: &syn::Type) -> syn::Type {
    if let syn::Type::Path(tp) = ty {
        if let Some(last) = tp.path.segments.last() {
            if last.ident == "Arc" {
                if let syn::PathArguments::AngleBracketed(ab) = &last.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = ab.args.first() {
                        return inner.clone();
                    }
                }
            }
        }
    }
    ty.clone()
}

// ===========================================================================
// #[lua_methods] — expose device methods to the daemon Lua REPL.
// ===========================================================================
//
// Apply to an `impl Type { ... }` block; tag each method to expose with
// `#[lua_method]`. The macro emits the original block plus an
// `impl LuaExposable for Type` whose `lua_methods()` returns a static
// slice of dispatchers.
//
// Accepted method shapes (first-cut subset; broaden later):
//
// - `&self` only (Arc-shared devices, no `&mut`)
// - args: f64, i64, u64, i32, u32, bool, String
// - return: any `serde::Serialize` type, `()`, or `Result<T, E>` where
//   `T: Serialize` and `E: ToString`
// - `async fn` is wrapped through `bsrs_core::bsrs_runtime().block_on`
//
// Unsupported shapes produce a compile error pointing at the method.

use syn::{ImplItem, ItemImpl, ReturnType, Type};

/// `#[lua_methods]` attribute on an `impl` block.
#[proc_macro_attribute]
pub fn lua_methods(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);
    let self_ty = &input.self_ty;

    // Generic impl blocks (`impl<T> Foo<T> { ... }`) would need a
    // distinct `&'static [LuaMethodEntry]` per monomorphization, but
    // statics can't capture generic parameters. Reject up front with
    // a clear message instead of emitting code that fails to compile
    // with cryptic errors. Future work: support via OnceLock-backed
    // per-T tables.
    if !input.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &input.generics,
            "#[lua_methods] does not support generic impl blocks; \
             concrete `impl Type` only. Wrap generic devices in a \
             concrete newtype, or expose individual instances.",
        )
        .to_compile_error()
        .into();
    }

    // Trait impls (`impl SomeTrait for Foo { ... }`) would silently
    // generate an `impl LuaExposable for Foo` while the user expected
    // the macro to operate on the trait. Two such tagged impls on
    // the same type would also conflict (only one `LuaExposable` impl
    // is allowed per type). Reject — `#[lua_methods]` is for inherent
    // `impl Type { ... }` blocks only.
    if let Some((_, trait_path, _)) = &input.trait_ {
        return syn::Error::new_spanned(
            trait_path.clone(),
            "#[lua_methods] applies to inherent impl blocks only \
             (`impl Type { ... }`), not trait impls. Move the tagged \
             methods into a separate inherent impl on the same type.",
        )
        .to_compile_error()
        .into();
    }

    let mut entries: Vec<TokenStream2> = Vec::new();

    for it in &input.items {
        let ImplItem::Fn(f) = it else { continue };
        let has_lua_method = f.attrs.iter().any(|a| a.path().is_ident("lua_method"));
        if !has_lua_method {
            continue;
        }
        let sig = &f.sig;
        let fn_name = &sig.ident;
        let fn_name_str = fn_name.to_string();
        let is_async = sig.asyncness.is_some();

        // Inputs: must start with `&self` (no &mut, no Self by value).
        let mut inputs = sig.inputs.iter();
        match inputs.next() {
            Some(syn::FnArg::Receiver(r)) if r.reference.is_some() && r.mutability.is_none() => {
                // ok
            }
            _ => {
                return syn::Error::new_spanned(
                    sig,
                    "#[lua_method] requires `&self` (no &mut, no Self by value)",
                )
                .to_compile_error()
                .into();
            }
        }
        let arg_types: Vec<&Type> = inputs
            .filter_map(|a| match a {
                syn::FnArg::Typed(t) => Some(&*t.ty),
                _ => None,
            })
            .collect();
        let arity = arg_types.len();

        // Build per-arg parsing: serde_json::from_value(args[i].clone())
        let mut arg_parses: Vec<TokenStream2> = Vec::with_capacity(arity);
        let mut arg_idents: Vec<TokenStream2> = Vec::with_capacity(arity);
        for (i, ty) in arg_types.iter().enumerate() {
            let ident = quote::format_ident!("arg{}", i);
            arg_parses.push(quote! {
                let #ident: #ty = ::bsrs::core::lua_exposable::__macro_support::serde_json::from_value(args[#i].clone())
                    .map_err(|e| format!(
                        concat!("lua_method '", #fn_name_str, "': arg #", stringify!(#i), ": {}"),
                        e
                    ))?;
            });
            arg_idents.push(quote! { #ident });
        }

        let call = if is_async {
            quote! {
                ::bsrs::core::runtime::bsrs_runtime()
                    .block_on(this_typed.#fn_name(#(#arg_idents),*))
            }
        } else {
            quote! { this_typed.#fn_name(#(#arg_idents),*) }
        };

        // Map the return into Result<serde_json::Value, String>.
        let return_handling = match &sig.output {
            ReturnType::Default => quote! {
                let _ = #call;
                Ok(::serde_json::Value::Null)
            },
            ReturnType::Type(_, ret_ty) => {
                if is_result_type(ret_ty) {
                    quote! {
                        let r = #call;
                        match r {
                            Ok(v) => ::bsrs::core::lua_exposable::__macro_support::serde_json::to_value(v).map_err(|e| e.to_string()),
                            Err(e) => Err(::std::string::ToString::to_string(&e)),
                        }
                    }
                } else {
                    quote! {
                        let v = #call;
                        ::bsrs::core::lua_exposable::__macro_support::serde_json::to_value(v).map_err(|e| e.to_string())
                    }
                }
            }
        };

        entries.push(quote! {
            ::bsrs::core::lua_exposable::LuaMethodEntry {
                name: #fn_name_str,
                arity: #arity,
                dispatch: |this, args| {
                    if args.len() != #arity {
                        return Err(format!(
                            "lua_method '{}': expected {} args, got {}",
                            #fn_name_str, #arity, args.len()
                        ));
                    }
                    let this_typed: &#self_ty = match this.downcast_ref::<#self_ty>() {
                        Some(v) => v,
                        None => return Err(format!(
                            "lua_method '{}': downcast failed (wrong concrete type)",
                            #fn_name_str
                        )),
                    };
                    #(#arg_parses)*
                    #return_handling
                },
            }
        });
    }

    // Strip `#[lua_method]` from the original impl so the compiler
    // doesn't error on an unknown attribute.
    let stripped_items: Vec<ImplItem> = input
        .items
        .iter()
        .map(|it| match it {
            ImplItem::Fn(f) => {
                let mut f = f.clone();
                f.attrs.retain(|a| !a.path().is_ident("lua_method"));
                ImplItem::Fn(f)
            }
            other => other.clone(),
        })
        .collect();

    let mut original = input.clone();
    original.items = stripped_items;

    let lua_impl = quote! {
        impl ::bsrs::core::lua_exposable::LuaExposable for #self_ty {
            fn lua_methods() -> &'static [::bsrs::core::lua_exposable::LuaMethodEntry] {
                static METHODS: &[::bsrs::core::lua_exposable::LuaMethodEntry] = &[
                    #(#entries),*
                ];
                METHODS
            }
        }
    };

    let expanded = quote! {
        #original
        #lua_impl
    };
    expanded.into()
}

/// True if the type literally starts with `Result` (we don't try to
/// resolve aliases). Good enough for `Result<T, E>` and
/// `bsrs_core::Result<T>` — the latter still has the `Result` ident.
fn is_result_type(ty: &Type) -> bool {
    if let Type::Path(tp) = ty {
        if let Some(last) = tp.path.segments.last() {
            return last.ident == "Result";
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::SignalAttr;
    use quote::quote;

    fn kind(tokens: proc_macro2::TokenStream) -> syn::Result<&'static str> {
        syn::parse2::<SignalAttr>(tokens).map(|a| a.kind)
    }

    #[test]
    fn signal_attr_kind_is_parsed_by_key_not_substring() {
        assert_eq!(
            kind(quote!(rw, "{prefix}:kind", kind = config)).unwrap(),
            "Config"
        );
        assert_eq!(kind(quote!("{prefix}:VAL")).unwrap(), "Normal");
        assert_eq!(
            kind(quote!(ro, "{prefix}:RBV", kind = Hinted)).unwrap(),
            "Hinted"
        );
        assert_eq!(
            kind(quote!(x, "{prefix}:X", kind = OMITTED,)).unwrap(),
            "Omitted"
        );
        let t = syn::parse2::<SignalAttr>(quote!(rw, "a\"b", kind = normal)).unwrap();
        assert_eq!(t.template.value(), "a\"b");
    }

    #[test]
    fn signal_attr_rejects_unknown_tokens() {
        assert!(kind(quote!(rw, "t", kind = hinte)).is_err());
        assert!(kind(quote!(rx, "t")).is_err());
        assert!(kind(quote!(rw, "t", knd = config)).is_err());
        assert!(kind(quote!(rw, "t", kind = config, extra)).is_err());
        assert!(kind(quote!(rw)).is_err());
    }
}
