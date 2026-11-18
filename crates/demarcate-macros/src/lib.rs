use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{Ident, ItemFn, LitBool, LitInt, LitStr, Token, Type, bracketed, parse_macro_input};

#[derive(Default)]
struct Arguments {
    propagation: Option<Ident>,
    isolation: Option<Ident>,
    timeout: Option<LitInt>,
    read_only: Option<bool>,
    rollback_for: Vec<Type>,
    no_rollback_for: Vec<Type>,
    rollback_for_name: Vec<LitStr>,
    no_rollback_for_name: Vec<LitStr>,
    transaction_manager: Option<LitStr>,
    label: Vec<LitStr>,
}

fn list<T: syn::parse::Parse>(meta: &syn::meta::ParseNestedMeta) -> syn::Result<Vec<T>> {
    let value = meta.value()?;
    if value.peek(syn::token::Bracket) {
        let content;
        bracketed!(content in value);
        Ok(Punctuated::<T, Token![,]>::parse_terminated(&content)?.into_iter().collect())
    } else {
        Ok(vec![value.parse()?])
    }
}

impl Arguments {
    fn parse(&mut self, meta: syn::meta::ParseNestedMeta) -> syn::Result<()> {
        let name = meta.path.get_ident().map(Ident::to_string).unwrap_or_default();
        match name.as_str() {
            "propagation" => self.propagation = Some(meta.value()?.parse()?),
            "isolation" => self.isolation = Some(meta.value()?.parse()?),
            "timeout" => self.timeout = Some(meta.value()?.parse()?),
            "read_only" => {
                self.read_only =
                    Some(if meta.input.peek(Token![=]) { meta.value()?.parse::<LitBool>()?.value } else { true })
            }
            "rollback_for" => self.rollback_for = list(&meta)?,
            "no_rollback_for" => self.no_rollback_for = list(&meta)?,
            "rollback_for_name" => self.rollback_for_name = list(&meta)?,
            "no_rollback_for_name" => self.no_rollback_for_name = list(&meta)?,
            "transaction_manager" => self.transaction_manager = Some(meta.value()?.parse()?),
            "label" => self.label = list(&meta)?,
            _ => {
                return Err(meta.error(
                    "unknown #[transactional] attribute; expected one of: propagation, isolation, timeout, \
                     read_only, rollback_for, no_rollback_for, rollback_for_name, no_rollback_for_name, \
                     transaction_manager, label",
                ));
            }
        }
        Ok(())
    }

    fn attribute(&self) -> TokenStream2 {
        let mut statements = Vec::new();
        if let Some(propagation) = &self.propagation {
            statements.push(quote! { attribute.definition.propagation = ::demarcate::Propagation::#propagation; });
        }
        if let Some(isolation) = &self.isolation {
            statements.push(quote! { attribute.definition.isolation = ::demarcate::Isolation::#isolation; });
        }
        if let Some(timeout) = &self.timeout {
            statements.push(quote! {
                attribute.definition.timeout = ::core::option::Option::Some(::core::time::Duration::from_secs(#timeout));
            });
        }
        if let Some(read_only) = self.read_only {
            statements.push(quote! { attribute.definition.read_only = #read_only; });
        }
        for error in &self.rollback_for {
            statements.push(quote! {
                attribute.rollback_rules.push(::demarcate::RollbackRule::rollback_for::<#error>());
            });
        }
        for pattern in &self.rollback_for_name {
            statements.push(quote! {
                attribute.rollback_rules.push(::demarcate::RollbackRule::rollback_for_name(#pattern));
            });
        }
        for error in &self.no_rollback_for {
            statements.push(quote! {
                attribute.rollback_rules.push(::demarcate::RollbackRule::no_rollback_for::<#error>());
            });
        }
        for pattern in &self.no_rollback_for_name {
            statements.push(quote! {
                attribute.rollback_rules.push(::demarcate::RollbackRule::no_rollback_for_name(#pattern));
            });
        }
        if let Some(qualifier) = &self.transaction_manager {
            statements.push(quote! {
                attribute.qualifier = ::core::option::Option::Some(::std::string::String::from(#qualifier));
            });
        }
        for label in &self.label {
            statements.push(quote! { attribute.labels.push(::std::string::String::from(#label)); });
        }
        quote! {{
            let mut attribute = ::demarcate::TransactionAttribute::new();
            #(#statements)*
            attribute
        }}
    }
}

#[proc_macro_attribute]
pub fn transactional(arguments: TokenStream, item: TokenStream) -> TokenStream {
    let mut parsed = Arguments::default();
    let parser = syn::meta::parser(|meta| parsed.parse(meta));
    parse_macro_input!(arguments with parser);
    let function = parse_macro_input!(item as ItemFn);

    if function.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            function.sig.fn_token,
            "#[transactional] requires an `async fn` that returns `Result<T, E>` where `E: From<TransactionError>`",
        )
        .to_compile_error()
        .into();
    }

    let ItemFn { attrs, vis, sig, block } = function;
    let name = &sig.ident;
    let attribute = parsed.attribute();
    quote! {
        #(#attrs)*
        #vis #sig {
            ::demarcate::invoke_within_transaction(
                #attribute,
                ::core::concat!(::core::module_path!(), "::", ::core::stringify!(#name)),
                ::std::boxed::Box::pin(async move #block),
            )
            .await
        }
    }
    .into()
}
