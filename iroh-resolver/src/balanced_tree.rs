use std::collections::VecDeque;

use anyhow::Result;
use async_stream::try_stream;
use bytes::{Bytes, BytesMut};
use cid::Cid;
use futures::{Stream, StreamExt};

use crate::unixfs::{dag_pb, unixfs_pb, DataType, Node, UnixfsNode};
use crate::unixfs_builder::encode_unixfs_pb;

/// Default degree number for balanced tree, taken from unixfs specs
/// https://github.com/ipfs/specs/blob/main/UNIXFS.md#layout
pub const DEFAULT_DEGREE: usize = 174;

#[derive(Debug, PartialEq, Eq)]
pub enum TreeBuilder {
    /// TreeBuilder that builds a "balanced tree" with a max degree size of
    /// degree
    Balanced { degree: usize },
}

impl TreeBuilder {
    pub fn balanced_tree() -> Self {
        Self::balanced_tree_with_degree(DEFAULT_DEGREE)
    }

    pub fn balanced_tree_with_degree(degree: usize) -> Self {
        assert!(degree > 1);
        TreeBuilder::Balanced { degree }
    }

    pub fn stream_tree(
        &self,
        chunks: impl Stream<Item = std::io::Result<BytesMut>>,
    ) -> impl Stream<Item = Result<(Cid, Bytes)>> {
        match self {
            TreeBuilder::Balanced { degree } => stream_balanced_tree(chunks, *degree),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct LinkInfo {
    cid: Cid,
    raw_data_len: u64,
    encoded_len: u64,
}

fn stream_balanced_tree(
    in_stream: impl Stream<Item = std::io::Result<BytesMut>>,
    degree: usize,
) -> impl Stream<Item = Result<(Cid, Bytes)>> {
    try_stream! {
        // degree = 8
        // VecDeque![ vec![] ]
        // ..
        // VecDeque![ vec![0, 1, 2, 3, 4, 5, 6, 7] ]
        // VecDeque![ vec![8], vec![p0] ]

        // ..

        // VecDeque![ vec![0, 1, 2, 3, 4, 5, 6, 7] vec![p0] ]
        // VecDeque![ vec![], vec![p0, p1]]

        // ..

        // VecDeque![ vec![0, 1, 2, 3, 4, 5, 6, 7] vec![p0, p1, p2, p3, p4, p5, p6, p7], ]
        // VecDeque![ vec![], vec![p0, p1, p2, p3, p4, p5, p6, p7], vec![] ]
        // VecDeque![ vec![8], vec![p8], vec![pp0] ]
        //
        // A vecdeque of vecs, the first vec representing the lowest layer of stem nodes
        // and the last vec representing the root node
        // Since we emit leaf and stem nodes as we go, we only need to keep track of the
        // most "recent" branch, storing the links to that node's children & yielding them
        // when each node reaches `degree` number of links
        let mut tree: VecDeque<Vec<LinkInfo>> = VecDeque::new();
        tree.push_back(Vec::with_capacity(degree));

        tokio::pin!(in_stream);

        while let Some(chunk) = in_stream.next().await {
            let chunk = chunk?;
            let chunk = chunk.freeze();
            let tree_len = tree.len();

            // check if the leaf node of the tree is full
            if tree[0].len() == degree {
                // if so, iterate through nodes
                for i in 0..tree_len {
                    // if we encounter any nodes that are not full, break
                    if tree[i].len() < degree {
                        break;
                    }

                    // in this case we have a full set of links & we are
                    // at the top of the tree. Time to make a new layer.
                    if i == tree_len - 1 {
                        tree.push_back(Vec::with_capacity(degree));
                    }

                    // create node, keeping the cid
                    let links = std::mem::replace(&mut tree[i], Vec::with_capacity(degree));
                    let (link_info, bytes) = TreeNode::Stem(links).encode()?;
                    yield (link_info.cid, bytes);

                    // add link_info to parent node
                    tree[i+1].push(link_info);
                }
                // at this point the tree will be able to recieve new links
                // without "overflowing", aka the leaf node and stem nodes
                // have fewer than `degree` number of links
            }

            // now that we know the tree is in a "healthy" state to
            // recieve more links, add the link to the tree
            let (link_info, bytes) = TreeNode::Leaf(chunk).encode()?;
            let cid = link_info.cid;
            tree[0].push(link_info);
            yield (cid, bytes);
            // at this point, the leaf node may have `degree` number of
            // links, but no other stem node will
        }

        // our stream had 1 chunk that we have already yielded
        if tree.len() == 1 && tree[0].len() == 1 {
            return
        }

        // clean up, aka yield the rest of the stem nodes
        // since all the stem nodes are able to recieve links
        // we don't have to worry about "overflow"
        while let Some(links) = tree.pop_front() {
            let (link_info, bytes) = TreeNode::Stem(links).encode()?;
            yield (link_info.cid, bytes);

            if let Some(front) = tree.front_mut() {
                front.push(link_info);
            } else {
                // final root, nothing to do
            }
        }
    }
}

fn create_unixfs_node_from_links(links: Vec<LinkInfo>) -> Result<UnixfsNode> {
    let blocksizes: Vec<u64> = links.iter().map(|l| l.raw_data_len).collect();
    let filesize: u64 = blocksizes.iter().sum();
    let links = links
        .into_iter()
        .map(|l| dag_pb::PbLink {
            hash: Some(l.cid.to_bytes()),
            /// ALL "stem" nodes have `name: None`.
            /// In kubo, nodes that have links to `leaf` nodes have `name: Some("".to_string())`
            name: None,
            /// tsize has no strict definition
            /// Iroh's definiton of `tsize` is "the cumulative size of the encoded tree
            /// pointed to by this link", so not just the size of the raw content, but including
            /// all encoded dag nodes as well.
            /// In the `go-merkledag` package, the `merkledag.proto` file, states that tsize
            /// is the "cumulative size of the target object"
            /// (https://github.com/ipfs/go-merkledag/blob/8335efd4765ed5a512baa7e522c3552d067cf966/pb/merkledag.proto#L29)
            tsize: Some(l.encoded_len as u64),
        })
        .collect();

    // PBNode.Data
    let inner = unixfs_pb::Data {
        r#type: DataType::File as i32,
        // total size of the raw data this node points to
        r#filesize: Some(filesize),
        // sizes of the raw data pointed to by each link in this node
        r#blocksizes,
        ..Default::default()
    };

    // create PBNode
    let outer = encode_unixfs_pb(&inner, links)?;

    // create UnixfsNode
    Ok(UnixfsNode::File(Node { inner, outer }))
}

// Leaf and Stem nodes are the two types of nodes that can exist in the tree
// Leaf nodes encode to `UnixfsNode::Raw`
// Stem nodes encode to `UnixfsNode::File`
enum TreeNode {
    Leaf(Bytes),
    Stem(Vec<LinkInfo>),
}

impl TreeNode {
    fn encode(self) -> Result<(LinkInfo, Bytes)> {
        match self {
            TreeNode::Leaf(bytes) => {
                let node = UnixfsNode::Raw(bytes);
                let (cid, bytes) = node.encode()?;
                Ok((
                    LinkInfo {
                        cid,
                        // in a leaf the raw data len and encoded len are the same since our leaf
                        // nodes are raw unixfs nodes
                        raw_data_len: bytes.len() as u64,
                        encoded_len: bytes.len() as u64,
                    },
                    bytes,
                ))
            }
            TreeNode::Stem(links) => {
                let mut encoded_len: u64 = links.iter().map(|l| l.encoded_len).sum();
                let node = create_unixfs_node_from_links(links)?;
                let (cid, bytes) = node.encode()?;
                encoded_len += bytes.len() as u64;
                let raw_data_len = node
                    .filesize()
                    .expect("UnixfsNode::File will have a filesize");
                Ok((
                    LinkInfo {
                        cid,
                        raw_data_len,
                        encoded_len,
                    },
                    bytes,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    // chunks are just a single usize integer
    const CHUNK_SIZE: u64 = std::mem::size_of::<usize>() as u64;

    fn test_chunk_stream(num_chunks: usize) -> impl Stream<Item = std::io::Result<BytesMut>> {
        futures::stream::iter((0..num_chunks).map(|n| Ok(BytesMut::from(&n.to_be_bytes()[..]))))
    }

    async fn build_expect_tree(num_chunks: usize, degree: usize) -> Vec<Vec<(Cid, Bytes)>> {
        let chunks = test_chunk_stream(num_chunks);
        tokio::pin!(chunks);
        let mut tree = vec![vec![]];
        let mut links = vec![vec![]];

        if num_chunks / degree == 0 {
            let chunk = chunks.next().await.unwrap().unwrap();
            let leaf = TreeNode::Leaf(chunk.freeze());
            let (link_info, bytes) = leaf.encode().unwrap();
            println!("len: {}", link_info.raw_data_len);
            tree[0].push((link_info.cid, bytes));
            return tree;
        }

        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.unwrap();
            let leaf = TreeNode::Leaf(chunk.freeze());
            let (link_info, bytes) = leaf.encode().unwrap();
            println!("len: {}", link_info.raw_data_len);
            tree[0].push((link_info.cid, bytes));
            links[0].push(link_info);
        }

        while tree.last().unwrap().len() > 1 {
            let prev_layer = links.last().unwrap();
            let count = prev_layer.len() / degree;
            let mut tree_layer = Vec::with_capacity(count);
            let mut links_layer = Vec::with_capacity(count);
            for links in prev_layer.chunks(degree) {
                let stem = TreeNode::Stem(links.to_vec());
                let (link_info, bytes) = stem.encode().unwrap();
                println!("len: {}", link_info.raw_data_len);
                tree_layer.push((link_info.cid, bytes));
                links_layer.push(link_info);
            }
            tree.push(tree_layer);
            links.push(links_layer);
        }
        tree
    }

    async fn build_expect_vec_from_tree(
        tree: Vec<Vec<(Cid, Bytes)>>,
        num_chunks: usize,
        degree: usize,
    ) -> Vec<(Cid, Bytes)> {
        let mut out = vec![];

        if num_chunks == 1 {
            out.push(tree[0][0].clone());
            return out;
        }

        let mut counts = vec![0; tree.len()];

        for leaf in tree[0].iter() {
            out.push(leaf.clone());
            counts[0] += 1;
            let mut push = counts[0] % degree == 0;
            for (num_layer, count) in counts.iter_mut().enumerate() {
                if num_layer == 0 {
                    continue;
                }
                if !push {
                    break;
                }
                out.push(tree[num_layer][*count].clone());
                *count += 1;
                if *count % degree != 0 {
                    push = false;
                }
            }
        }

        for (num_layer, count) in counts.into_iter().enumerate() {
            if num_layer == 0 {
                continue;
            }
            let layer = tree[num_layer].clone();
            for node in layer.into_iter().skip(count) {
                out.push(node);
            }
        }

        out
    }

    async fn build_expect(num_chunks: usize, degree: usize) -> Vec<(Cid, Bytes)> {
        let tree = build_expect_tree(num_chunks, degree).await;
        println!("{:?}", tree);
        build_expect_vec_from_tree(tree, num_chunks, degree).await
    }

    fn make_leaf(data: usize) -> (LinkInfo, Bytes) {
        TreeNode::Leaf(BytesMut::from(&data.to_be_bytes()[..]).freeze())
            .encode()
            .unwrap()
    }

    fn make_stem(links: Vec<LinkInfo>) -> (LinkInfo, Bytes) {
        TreeNode::Stem(links).encode().unwrap()
    }

    #[tokio::test]
    async fn test_build_expect() {
        // manually build tree made of 7 chunks (11 total nodes)
        let (li_0, bytes_0) = make_leaf(0);
        let (li_1, bytes_1) = make_leaf(1);
        let (li_2, bytes_2) = make_leaf(2);
        let cid_0 = li_0.cid;
        let cid_1 = li_1.cid;
        let cid_2 = li_2.cid;
        let (stem_li_0, stem_bytes_0) = make_stem(vec![li_0, li_1, li_2]);
        let stem_cid_0 = stem_li_0.cid;
        let (li_3, bytes_3) = make_leaf(3);
        let (li_4, bytes_4) = make_leaf(4);
        let (li_5, bytes_5) = make_leaf(5);
        let cid_3 = li_3.cid;
        let cid_4 = li_4.cid;
        let cid_5 = li_5.cid;
        let (stem_li_1, stem_bytes_1) = make_stem(vec![li_3, li_4, li_5]);
        let stem_cid_1 = stem_li_1.cid;
        let (li_6, bytes_6) = make_leaf(6);
        let cid_6 = li_6.cid;
        let (stem_li_2, stem_bytes_2) = make_stem(vec![li_6]);
        let stem_cid_2 = stem_li_2.cid;
        let (root_li, root_bytes) = make_stem(vec![stem_li_0, stem_li_1, stem_li_2]);
        let leaf_0 = (cid_0, bytes_0);
        let leaf_1 = (cid_1, bytes_1);
        let leaf_2 = (cid_2, bytes_2);
        let leaf_3 = (cid_3, bytes_3);
        let leaf_4 = (cid_4, bytes_4);
        let leaf_5 = (cid_5, bytes_5);
        let leaf_6 = (cid_6, bytes_6);

        let stem_0 = (stem_cid_0, stem_bytes_0);
        let stem_1 = (stem_cid_1, stem_bytes_1);
        let stem_2 = (stem_cid_2, stem_bytes_2);

        let root = (root_li.cid, root_bytes);

        let expect_tree = vec![
            vec![
                leaf_0.clone(),
                leaf_1.clone(),
                leaf_2.clone(),
                leaf_3.clone(),
                leaf_4.clone(),
                leaf_5.clone(),
                leaf_6.clone(),
            ],
            vec![stem_0.clone(), stem_1.clone(), stem_2.clone()],
            vec![root.clone()],
        ];
        let got_tree = build_expect_tree(7, 3).await;
        assert_eq!(expect_tree, got_tree);

        let expect_vec = vec![
            leaf_0, leaf_1, leaf_2, stem_0, leaf_3, leaf_4, leaf_5, stem_1, leaf_6, stem_2, root,
        ];
        let got_vec = build_expect_vec_from_tree(got_tree, 7, 3).await;
        assert_eq!(expect_vec, got_vec);
    }

    async fn ensure_equal(
        expect: Vec<(Cid, Bytes)>,
        got: impl Stream<Item = Result<(Cid, Bytes)>>,
        expected_filesize: u64,
    ) {
        let mut i = 0;
        tokio::pin!(got);
        let mut got_filesize = 0;
        let mut expected_tsize = 0;
        let mut got_tsize = 0;
        while let Some(node) = got.next().await {
            let (expect_cid, expect_bytes) = expect
                .get(i)
                .expect("too many nodes in balanced tree stream");
            let node = node.expect("unexpected error in balanced tree stream");
            let (got_cid, got_bytes) = node;
            let len = got_bytes.len() as u64;
            println!("node index {}", i);
            i += 1;
            let expect_node = UnixfsNode::decode(expect_cid, expect_bytes.to_owned()).unwrap();
            let got_node = UnixfsNode::decode(&got_cid, got_bytes).unwrap();
            if let Some(DataType::File) = got_node.typ() {
                assert_eq!(
                    got_node.filesize().unwrap(),
                    got_node.blocksizes().unwrap().iter().sum::<u64>()
                );
            }
            assert_eq!(expect_node, got_node);
            if expect.len() == i {
                got_tsize = got_node.links().map(|l| l.unwrap().tsize.unwrap()).sum();
                got_filesize = got_node.filesize().unwrap();
            } else {
                expected_tsize += len;
            }
        }
        if expect.len() != i {
            panic!(
                "expected at {} nodes of the stream, got {}",
                expect.len(),
                i
            );
        }
        assert_eq!(expected_filesize, got_filesize);
        assert_eq!(expected_tsize, got_tsize);
    }

    #[tokio::test]
    async fn balanced_tree_test_leaf() {
        let num_chunks = 1;
        let expect = build_expect(num_chunks, 3).await;
        let got = stream_balanced_tree(test_chunk_stream(1), 3);
        tokio::pin!(got);
        ensure_equal(expect, got, num_chunks as u64 * CHUNK_SIZE).await;
    }

    #[tokio::test]
    async fn balanced_tree_test_height_one() {
        let num_chunks = 3;
        let degrees = 3;
        let expect = build_expect(num_chunks, degrees).await;
        let got = stream_balanced_tree(test_chunk_stream(num_chunks), degrees);
        tokio::pin!(got);
        ensure_equal(expect, got, num_chunks as u64 * CHUNK_SIZE).await;
    }

    #[tokio::test]
    async fn balanced_tree_test_height_two_full() {
        let degrees = 3;
        let num_chunks = 9;
        let expect = build_expect(num_chunks, degrees).await;
        let got = stream_balanced_tree(test_chunk_stream(num_chunks), degrees);
        tokio::pin!(got);
        ensure_equal(expect, got, num_chunks as u64 * CHUNK_SIZE).await;
    }

    #[tokio::test]
    async fn balanced_tree_test_height_two_not_full() {
        let degrees = 3;
        let num_chunks = 10;
        let expect = build_expect(num_chunks, degrees).await;
        let got = stream_balanced_tree(test_chunk_stream(num_chunks), degrees);
        tokio::pin!(got);
        ensure_equal(expect, got, num_chunks as u64 * CHUNK_SIZE).await;
    }

    #[tokio::test]
    async fn balanced_tree_test_height_three() {
        let num_chunks = 125;
        let degrees = 5;
        let expect = build_expect(num_chunks, degrees).await;
        let got = stream_balanced_tree(test_chunk_stream(num_chunks), degrees);
        tokio::pin!(got);
        ensure_equal(expect, got, num_chunks as u64 * CHUNK_SIZE).await;
    }

    #[tokio::test]
    async fn balanced_tree_test_large() {
        let num_chunks = 780;
        let degrees = 11;
        let expect = build_expect(num_chunks, degrees).await;
        let got = stream_balanced_tree(test_chunk_stream(num_chunks), degrees);
        tokio::pin!(got);
        ensure_equal(expect, got, num_chunks as u64 * CHUNK_SIZE).await;
    }
}
