use zsync::rcksum::map::*;
use zsync::rcksum::types::*;

fn main() {
    let mut map = ZBlockMap::new(255*2);

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
    for i in 0..255 {
        map.insert(
            i,
            ZBlock {
                rsum: my_rsums[i % 7],
                checksum: PartialChecksum {
                    value: MD4Digest([1, 2, 3, 4, i as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                    length: 5,
                },
            },
        );
    }
    for i in 0..255 {
        map.insert(
            i,
            ZBlock {
                rsum: my_rsums[i % 7],
                checksum: PartialChecksum {
                    value: MD4Digest([1, 2, 3, i as u8, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                    length: 5,
                },
            },
        );
    }
    for i in 0..255 {
        map.insert(
            i,
            ZBlock {
                rsum: my_rsums[i % 7],
                checksum: PartialChecksum {
                    value: MD4Digest([1, 2, i as u8, 4, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                    length: 5,
                },
            },
        );
    }

    println!("Searching for weak misses 60000 times...");
    for i in 0..60000 {
        map.search_weak(Rsum(9, 9));
    }

    println!("Searching for strong misses 60000 times...");
    for j in 0..300 {
        for i in 0..200 {
            let result = map.search_weak(my_rsums[i % 7]).unwrap();
            result.get(&PartialChecksum {
                value: MD4Digest([1, 2, 3, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                length: 5,
            });
        }
    }

    println!("Searching for hits 30600000 times...");
    for j in 0..40000 {
        for i in 0..255 {
            let result = map.search_weak(my_rsums[i % 7]).unwrap();
            result
                .get(&PartialChecksum {
                    value: MD4Digest([1, 2, 3, 4, i as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                    length: 5,
                })
                .unwrap();
        }
        for i in 0..255 {
            let result = map.search_weak(my_rsums[i % 7]).unwrap();
            result
                .get(&PartialChecksum {
                    value: MD4Digest([1, 2, 3, i as u8, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                    length: 5,
                })
                .unwrap();
        }
        for i in 0..255 {
            let result = map.search_weak(my_rsums[i % 7]).unwrap();
            result
                .get(&PartialChecksum {
                    value: MD4Digest([1, 2, i as u8, 4, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                    length: 5,
                })
                .unwrap();
        }
    }
    println!("Done!");
}
