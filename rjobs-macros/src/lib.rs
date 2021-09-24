use proc_macro::TokenStream;
use quote::quote;

#[proc_macro_attribute]
pub fn job(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut prefix = TokenStream::from(quote! {
      #[rjobs::async_trait::async_trait]
      #[rjobs::typetag::serde]
    });
    prefix.extend(item);
    prefix
}
