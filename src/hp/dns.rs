use trust_dns_resolver::TokioAsyncResolver;

use once_cell::sync::Lazy;
pub static DNS_RESOLVER: Lazy<TokioAsyncResolver> = Lazy::new(|| {
    TokioAsyncResolver::tokio_from_system_conf().expect("unable to create DNS resolver")
});

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_dns_lookup() {
        let res = DNS_RESOLVER.lookup_ip("derp.iroh.computer").await.unwrap();
        let res: Vec<_> = res.iter().collect();
        assert!(!res.is_empty());
        dbg!(res);
    }
}
