use std::time::Instant;

use cid::multihash::{Code, MultihashDigest};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use iroh_metrics::config::Config as MetricsConfig;
use iroh_metrics::store::Metrics;
use iroh_rpc_client::Config as RpcClientConfig;
use iroh_rpc_types::Addr;
use iroh_store::{Config, Store};
use tokio::runtime::Runtime;

const RAW: u64 = 0x55;

pub fn put_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_put");
    for value_size in [32, 128, 512, 1024].iter() {
        let value = vec![8u8; *value_size];
        let hash = Code::Sha2_256.digest(&value);
        let key = cid::Cid::new_v1(RAW, hash);

        group.throughput(criterion::Throughput::Bytes(*value_size as u64));
        group.bench_with_input(
            BenchmarkId::new("value_size", *value_size as u64),
            &(key, value),
            |b, (key, value)| {
                let executor = Runtime::new().unwrap();
                let dir = tempfile::tempdir().unwrap();
                let rpc_client = RpcClientConfig::default();
                let (rpc_addr, _) = Addr::new_mem();
                let config = Config {
                    path: dir.path().into(),
                    rpc_addr,
                    rpc_client,
                    metrics: MetricsConfig::default(),
                };
                let metrics = Metrics::default();
                let store =
                    executor.block_on(async { Store::create(config, metrics).await.unwrap() });
                let store_ref = &store;
                b.to_async(&executor).iter(|| async move {
                    store_ref.put(*key, black_box(value), []).await.unwrap()
                });
            },
        );
    }
    group.finish();
}

pub fn get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_get");
    for value_size in [32, 128, 512, 1024].iter() {
        group.throughput(criterion::Throughput::Bytes(*value_size as u64));
        group.bench_with_input(
            BenchmarkId::new("value_size", *value_size as u64),
            &(),
            |b, _| {
                let executor = Runtime::new().unwrap();
                let dir = tempfile::tempdir().unwrap();
                let rpc_client = RpcClientConfig::default();
                let (rpc_addr, _) = Addr::new_mem();
                let config = Config {
                    path: dir.path().into(),
                    rpc_addr,
                    rpc_client,
                    metrics: MetricsConfig::default(),
                };
                let metrics = Metrics::default();
                let store =
                    executor.block_on(async { Store::create(config, metrics).await.unwrap() });
                let store_ref = &store;
                let keys = executor.block_on(async {
                    let mut keys = Vec::new();
                    for i in 0..1000 {
                        let value = vec![i as u8; *value_size];
                        let hash = Code::Sha2_256.digest(&value);
                        let key = cid::Cid::new_v1(RAW, hash);
                        keys.push(key);
                        store_ref.put(key, &value, []).await.unwrap();
                    }
                    keys
                });

                let keys_ref = &keys[..];
                b.to_async(&executor).iter_custom(|iters| async move {
                    let l = keys_ref.len();

                    let start = Instant::now();
                    for i in 0..iters {
                        let key = &keys_ref[(i as usize) % l];
                        let res = store_ref.get(key).await.unwrap().unwrap();
                        black_box(res);
                    }
                    start.elapsed()
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, put_benchmark, get_benchmark);
criterion_main!(benches);
