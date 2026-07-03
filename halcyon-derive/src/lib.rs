//! `#[derive(FromRow)]` for the Halcyon Db2 client.
//!
//! Generates an `impl halcyon::FromRow` that maps a result-set row into a struct
//! **by position** — column 0 into the first field, column 1 into the second,
//! and so on — the Rust counterpart of the C++ `HALCYON_REFLECT` / `map_row`
//! machinery. The column count must equal the field count, or a `Mapping` error
//! is returned. Each field type must implement `halcyon::FromColumn`.
//!
//! This crate emits code that refers to the `halcyon` crate as `::halcyon`, so a
//! consumer must depend on `halcyon` (which re-exports this macro under the
//! `derive` feature). It is an implementation detail of `halcyon` and carries no
//! stability guarantee of its own.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

/// Derives `halcyon::FromRow`, mapping a row's columns into the struct's fields
/// by position.
#[proc_macro_derive(FromRow)]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let data = match &input.data {
        Data::Struct(data) => data,
        Data::Enum(_) => {
            return Err(syn::Error::new_spanned(
                name,
                "FromRow cannot be derived for enums; it maps one result-set row \
                 into a struct's fields by position",
            ));
        }
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                name,
                "FromRow cannot be derived for unions",
            ));
        }
    };

    // Build the struct literal that reads each field from its positional column.
    // Field types must implement `FromColumn`; the target type is inferred from
    // the field, so `__row.column(i)?` resolves it.
    let (field_count, body) = match &data.fields {
        Fields::Named(fields) => {
            let field_count = fields.named.len();
            let inits = fields.named.iter().enumerate().map(|(i, f)| {
                let ident = f.ident.as_ref().expect("named field has an ident");
                quote! { #ident: __row.column(#i)? }
            });
            (field_count, quote! { Self { #(#inits),* } })
        }
        Fields::Unnamed(fields) => {
            let field_count = fields.unnamed.len();
            let inits = (0..field_count).map(|i| quote! { __row.column(#i)? });
            (field_count, quote! { Self(#(#inits),*) })
        }
        Fields::Unit => {
            return Err(syn::Error::new_spanned(
                name,
                "FromRow cannot be derived for a unit struct (there are no fields \
                 to map columns into)",
            ));
        }
    };

    let name_str = name.to_string();

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::halcyon::FromRow for #name #ty_generics #where_clause {
            fn from_row(__row: &::halcyon::Row<'_>) -> ::halcyon::Result<Self> {
                let __n = __row.column_count();
                if __n != #field_count {
                    return ::core::result::Result::Err(::halcyon::Error::new(
                        ::halcyon::ErrorCode::Mapping,
                        ::std::format!(
                            "FromRow for `{}`: expected {} column(s), got {}",
                            #name_str,
                            #field_count,
                            __n
                        ),
                    ));
                }
                ::core::result::Result::Ok(#body)
            }
        }
    })
}
