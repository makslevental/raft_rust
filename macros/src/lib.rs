extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Error};

#[proc_macro_derive(FromTuple)]
pub fn from_tuple(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    if let Data::Struct(data) = &input.data {
        let struct_ident = &input.ident;
        let fields = &data.fields;
        let dvars = (0..fields.len())
            .map(|i| Ident::new(&format!("d{}", i), Span::call_site()))
            .collect::<Vec<_>>();

        let idents = fields.iter().map(|f| f.ident.as_ref());
        let types = fields.iter().map(|f| &f.ty);

        let tuple_type = quote! { (#(#types),*) };
        let destructed = quote! { (#(#dvars),*) };

        quote! {
            impl From<#tuple_type> for #struct_ident {

                #[inline]
                fn from(tuple: #tuple_type) -> Self {
                    let #destructed = tuple;

                    Self {
                        #(#idents: #dvars),*
                    }
                }
            }
        }
    } else {
        Error::new_spanned(input, "FromTuple currently only supports Struct").to_compile_error()
    }
    .into()
}
