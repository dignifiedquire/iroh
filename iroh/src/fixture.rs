use std::collections::HashMap;
use std::env;

use futures::StreamExt;
use iroh_api::{Lookup, MockApi, MockP2p, OutType, PeerId};
use relative_path::RelativePathBuf;

type GetFixture = fn() -> MockApi;
type FixtureRegistry = HashMap<String, GetFixture>;

fn fixture_lookup() -> MockApi {
    let mut api = MockApi::default();
    api.expect_p2p().returning(|| {
        let mut mock_p2p = MockP2p::default();

        mock_p2p.expect_lookup().returning(|_addr| {
            let peer_id = "1AXRDqR8jTkwzGqyu3qknicAC5X578zTMxhAi2brppK2bB"
                .parse::<PeerId>()
                .unwrap();
            Ok(Lookup {
                peer_id,
                listen_addrs: vec![],
                local_addrs: vec![],
            })
        });
        Ok(mock_p2p)
    });
    api
}

fn fixture_get() -> MockApi {
    let mut api = MockApi::default();
    api.expect_get_stream().returning(|_ipfs_path| {
        futures::stream::iter(vec![
            Ok((RelativePathBuf::from_path("a").unwrap(), OutType::Dir)),
            Ok((
                RelativePathBuf::from_path("b").unwrap(),
                OutType::Reader(Box::new(std::io::Cursor::new("hello"))),
            )),
        ])
        .boxed_local()
    });
    api
}

fn register_fixtures() -> FixtureRegistry {
    [
        ("lookup".to_string(), fixture_lookup as GetFixture),
        ("get".to_string(), fixture_get as GetFixture),
    ]
    .into_iter()
    .collect()
}

pub fn get_fixture_api() -> MockApi {
    let registry = register_fixtures();
    let fixture_name = env::var("IROH_CTL_FIXTURE").expect("IROH_CTL_FIXTURE must be set");
    let fixture = registry
        .get(&fixture_name)
        .unwrap_or_else(|| panic!("unknown fixture: {}", fixture_name));
    fixture()
}
