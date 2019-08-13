extern crate test;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use fnv::*;

use crate::error::*;
use super::types::*;

pub type ZBlockId = usize;

// Too much for zblock.
#[derive(Copy, Clone, Debug, Default)]
pub struct ZBlock {
    pub rsum: Rsum,
    pub checksum: PartialChecksum,
}

pub type ChecksumMap = HashMap<PartialChecksum, Vec<ZBlockId>, BuildHasherDefault<FnvHasher>>;

#[derive(Clone)]
pub struct ZBlockMap {
    pub rsum_map: HashMap<Rsum, ChecksumMap, BuildHasherDefault<FnvHasher>>,
    pub blocklist: Vec<ZBlock>,
}

impl ZBlockMap {
    pub fn new(num_blocks: usize) -> Self {
        ZBlockMap {
            rsum_map: HashMap::default(),
            blocklist: vec![ZBlock::default(); num_blocks],
        }
    }

    pub fn search_weak(&self, rsum: Rsum) -> Option<&ChecksumMap> {
        self.rsum_map.get(&rsum)
    }

    pub fn insert(&mut self, block_id: ZBlockId, block: ZBlock) {
        assert!(block_id < self.blocklist.len());
        self.blocklist[block_id] = block;

        let checksum_map = self.rsum_map.entry(block.rsum).or_insert(HashMap::default());
        let block_list = checksum_map.entry(block.checksum).or_insert(Vec::new());
        block_list.push(block_id);
    }

    pub fn remove_block(&mut self, block_id: ZBlockId) {
        let block = self.blocklist[block_id];
        let checksum_map = self.rsum_map.get_mut(&block.rsum).unwrap();
        let block_list = checksum_map.get_mut(&block.checksum).unwrap();
        block_list.remove_item(&block_id);

        if block_list.is_empty() {
            checksum_map.remove(&block.checksum);
        }

        if checksum_map.is_empty() {
            self.rsum_map.remove(&block.rsum);
        }
    }

    pub fn remove_checksum(&mut self, rsum: Rsum, checksum: PartialChecksum) {
        let checksum_map = self.rsum_map.get_mut(&rsum).unwrap();
        checksum_map.remove(&checksum);

        if checksum_map.is_empty() {
            self.rsum_map.remove(&rsum);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[test]
    fn sanity() {
        let mut map = ZBlockMap::new(10);
        map.insert(
            0,
            ZBlock {
                rsum: Rsum(1, 2),
                checksum: PartialChecksum {
                    value: [1; 16].into(),
                    length: 5,
                },
            },
        );
        map.insert(
            1,
            ZBlock {
                rsum: Rsum(1, 2),
                checksum: PartialChecksum {
                    value: [2; 16].into(),
                    length: 5,
                },
            },
        );
        map.insert(
            2,
            ZBlock {
                rsum: Rsum(3, 2),
                checksum: PartialChecksum {
                    value: [3; 16].into(),
                    length: 5,
                },
            },
        );
        map.insert(
            3,
            ZBlock {
                rsum: Rsum(1, 2),
                checksum: PartialChecksum {
                    value: [1; 16].into(),
                    length: 5,
                },
            },
        );

        let result = map.search_weak(Rsum(1, 2)).unwrap();
        assert!(result.len() == 2);
        assert!(
            result.get(&PartialChecksum {
                value: [1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].into(),
                length: 5
            }) == Some(&vec![0, 3])
        );
        assert!(
            result.get(&PartialChecksum {
                value: [2, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].into(),
                length: 5
            }) == Some(&vec![1])
        );

        let result = map.search_weak(Rsum(3, 2)).unwrap();
        assert!(result.len() == 1);
        assert!(
            result.get(&PartialChecksum {
                value: [3, 3, 3, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].into(),
                length: 5
            }) == Some(&vec![2])
        );

        let result = map.search_weak(Rsum(255, 2));
        assert!(result.is_none());

        map.remove_block(0);
        let result = map.search_weak(Rsum(1, 2)).unwrap();
        assert!(result.len() == 2);
        assert!(
            result.get(&PartialChecksum {
                value: [1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].into(),
                length: 5
            }) == Some(&vec![3])
        );
        assert!(
            result.get(&PartialChecksum {
                value: [2, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].into(),
                length: 5
            }) == Some(&vec![1])
        );

        map.remove_block(1);
        let result = map.search_weak(Rsum(1, 2)).unwrap();
        assert!(result.len() == 1);
        assert!(
				result.get(&PartialChecksum {
                value: [1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].into(),
                length: 5
            }) == Some(&vec![3])
        );

        map.remove_block(3);
        assert!(map.search_weak(Rsum(1, 2)).is_none());

        let result = map.search_weak(Rsum(3, 2)).unwrap();
        assert!(result.len() == 1);
        assert!(
            result.get(&PartialChecksum {
                value: [3, 3, 3, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].into(),
                length: 5
            }) == Some(&vec![2])
        );

        map.remove_block(2);
        assert!(map.search_weak(Rsum(3, 2)).is_none());
    }

    #[bench]
    fn bench(b: &mut Bencher) {
        let mut map = ZBlockMap::new(200);

        let my_rsums = vec![
            Rsum(1, 2),
            Rsum(2, 3),
            Rsum(3, 4),
            Rsum(4, 5),
            Rsum(5, 6),
            Rsum(6, 7),
            Rsum(7, 8),
        ];

        println!("Inserting blocks and building hash map...");
        for i in 0..200 {
            map.insert(
                i,
                ZBlock {
                    rsum: my_rsums[i % 7],
                    checksum: PartialChecksum {
                        value: [1, 2, 3, 4, i as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].into(),
                        length: 5,
                    },
                },
            );
        }

        b.iter( || {
            println!("Searching for weak misses 60000 times...");
            for _i in 0..60000 {
                map.search_weak(Rsum(9, 9));
            }

            println!("Searching for strong misses 60000 times...");
            for _j in 0..300 {
                for i in 0..200 {
                    let result = map.search_weak(my_rsums[i % 7]).unwrap();
                    result.get(&PartialChecksum {
                        value: [1, 2, 3, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].into(),
                        length: 5,
                    }).unwrap();
                }
            }

            println!("Searching for hits 60000 times...");
            for _j in 0..4000 {
                for i in 0..200 {
                    let result = map.search_weak(my_rsums[i % 7]).unwrap();
                    result
                        .get(&PartialChecksum {
                            value: [1, 2, 3, 4, i as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].into(),
                            length: 5,
                        }).unwrap();
                }
            }
            println!("Done!");
        });
    }
}
