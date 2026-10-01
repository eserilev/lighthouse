//! `#[feature_dispatch]` routes a function to its experimental feature copy while the feature is
//! active.
//!
//! ```ignore
//! #[feature_dispatch(Eip1234 => features::eip1234::process_foo, spec = spec, epoch = state.current_epoch())]
//! pub fn process_foo<E: EthSpec>(state: &mut BeaconState<E>, spec: &ChainSpec) -> Result<(), Error> {
//!     // original body
//! }
//! ```
//!
//! expands to:
//!
//! ```ignore
//! pub fn process_foo<E: EthSpec>(state: &mut BeaconState<E>, spec: &ChainSpec) -> Result<(), Error> {
//!     let active = spec.feature_enabled::<Eip1234>(state.current_epoch());
//!     if let Some(active) = active {
//!         return features::eip1234::process_foo(state, spec, active);
//!     }
//!     // original body
//! }
//! ```
//!
//! The copy takes the same arguments, `self` first for a method, then the `Active` token.

use proc_macro::{Span, TokenStream};
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{Error, Expr, ExprPath, FnArg, Ident, ItemFn, Pat, Path, Token, parse_macro_input};

struct Dispatch {
    feature: Path,
    copy: ExprPath,
    spec: Expr,
    epoch: Expr,
}

impl Parse for Dispatch {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let feature = input.parse()?;
        input.parse::<Token![=>]>()?;
        let copy = input.parse()?;
        let mut spec = None;
        let mut epoch = None;
        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            let slot = match key.to_string().as_str() {
                "spec" => &mut spec,
                "epoch" => &mut epoch,
                _ => return Err(Error::new(key.span(), "expected `spec` or `epoch`")),
            };
            if slot.replace(input.parse()?).is_some() {
                return Err(Error::new(key.span(), format!("duplicate `{key}`")));
            }
        }
        let missing = |key| Error::new(input.span(), format!("missing `{key} = <expr>`"));
        Ok(Dispatch {
            feature,
            copy,
            spec: spec.ok_or_else(|| missing("spec"))?,
            epoch: epoch.ok_or_else(|| missing("epoch"))?,
        })
    }
}

/// A copy with other arguments than the original does not compile:
///
/// ```compile_fail,E0061
/// # use feature_dispatch::feature_dispatch;
/// # use std::marker::PhantomData;
/// # struct Toy;
/// # struct Active<F>(PhantomData<F>);
/// # struct Spec;
/// # impl Spec {
/// #     fn feature_enabled<F>(&self, _epoch: u64) -> Option<Active<F>> {
/// #         Some(Active(PhantomData))
/// #     }
/// # }
/// fn copy(base: u64, _active: Active<Toy>) -> u64 {
///     base * 2
/// }
///
/// #[feature_dispatch(Toy => copy, spec = spec, epoch = epoch)]
/// fn reward(base: u64, spec: &Spec, epoch: u64) -> u64 {
///     base
/// }
/// ```
///
/// ```compile_fail,E0308
/// # use feature_dispatch::feature_dispatch;
/// # use std::marker::PhantomData;
/// # struct Toy;
/// # struct Active<F>(PhantomData<F>);
/// # struct Spec;
/// # impl Spec {
/// #     fn feature_enabled<F>(&self, _epoch: u64) -> Option<Active<F>> {
/// #         Some(Active(PhantomData))
/// #     }
/// # }
/// fn copy(base: u32, _spec: &Spec, _epoch: u64, _active: Active<Toy>) -> u64 {
///     u64::from(base) * 2
/// }
///
/// #[feature_dispatch(Toy => copy, spec = spec, epoch = epoch)]
/// fn reward(base: u64, spec: &Spec, epoch: u64) -> u64 {
///     base
/// }
/// ```
#[proc_macro_attribute]
pub fn feature_dispatch(attr: TokenStream, item: TokenStream) -> TokenStream {
    let Dispatch {
        feature,
        copy,
        spec,
        epoch,
    } = parse_macro_input!(attr as Dispatch);
    let mut function = parse_macro_input!(item as ItemFn);

    let arguments = match function
        .sig
        .inputs
        .iter()
        .map(|argument| match argument {
            FnArg::Receiver(receiver) => Ok(Ident::new("self", receiver.self_token.span)),
            FnArg::Typed(typed) => match &*typed.pat {
                Pat::Ident(pat) => Ok(pat.ident.clone()),
                pat => Err(Error::new(
                    pat.span(),
                    "feature_dispatch needs a name for each argument",
                )),
            },
        })
        .collect::<syn::Result<Vec<_>>>()
    {
        Ok(arguments) => arguments,
        Err(e) => return e.to_compile_error().into(),
    };

    let active = Ident::new("active", Span::mixed_site().into());
    let body = &function.block;
    function.block = syn::parse_quote!({
        let #active = (#spec).feature_enabled::<#feature>(#epoch);
        if let Some(#active) = #active {
            return #copy(#(#arguments,)* #active);
        }
        #body
    });
    quote!(#function).into()
}
