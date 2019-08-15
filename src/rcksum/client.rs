use std::fs::File;
use std::io::{Write, Seek, SeekFrom};
use std::path::Path;
use std::convert::TryInto;
use crate::error::*;
use super::types::*;
use super::map::*;
use super::data_window::*;

#[derive(Copy, Clone)]
pub struct Config {
    seq_matches: usize, // Must be > 0
    checksum_bytes: usize,
    blocksize: usize,
}

struct Context {
    config: Config,
    rsums: [Rsum; 2],
    num_blocks: usize,
    skip_bytes: usize,
    offset: usize,
    blockmap: ZBlockMap,
    file: File,
    blockshift: i32,
    next_match: Option<ZBlockId>,
}

impl Context {
    pub fn new(config: Config, num_blocks: usize, output_path: &Path) -> Result<Self> {
        assert!(config.seq_matches >= 1);

        // Create an empty file to fill in
        let mut file = File::create(output_path)?;
        for _i in 0..num_blocks * config.blocksize {
            file.write(&[0])?;
        }

        Ok(Context {
            config,
            rsums: [Rsum::default(), Rsum::default()],
            num_blocks,
            skip_bytes: 0,
            offset: 0,
            blockmap: ZBlockMap::new(num_blocks),
            file,
            // TODO: Calculate this properly
            blockshift: (config.blocksize as f64).log2().round() as i32,
            next_match: None,
        })
    }

    fn write_blocks(&mut self, blocks: &[ZBlockId], data: &[u8]) -> Result<()> {
        assert!(data.len() == self.config.blocksize);
        dbg!(blocks);
        for b in blocks {
            dbg!(b);
            // Calculate offset into the file from the block ID
            let offset = b * self.config.blocksize;
            dbg!(offset);
            let file_offset = self.file.seek(SeekFrom::Start(offset.try_into().unwrap()))?;
            assert!(file_offset as usize == offset);
            let bytes_written = self.file.write(data)?;
            assert!(bytes_written == data.len());
            self.blockmap.remove_block(*b);
        }
        Ok(())
    }

    fn check_block_match(&mut self, data: &[u8]) -> Result<Option<Vec<ZBlockId>>> {
        // Look up the rsum in the blockmap
        let rsum = self.rsums[0];
        dbg!(rsum);

        let blockmap = &self.blockmap;

        let blocks = {
            let checksum_map = blockmap.search_weak(rsum);

            if let Some(map) = checksum_map {
                // Weak hash hit; calculate the MD4 of this block
                let checksum = PartialChecksum {
                    value: MD4Digest::calculate(data),
                    length: self.config.checksum_bytes,
                };
                dbg!(checksum);
                map.get(&checksum).clone()
            } else {
                return Ok(None);
            }
        };

        // Look up the strong hash in the blockmap
        if let Some(b) = blocks {
            // Strong hash hit, write through all the blocks
            dbg!(b);
            return Ok(Some(b.clone()));
        } else {
            return Ok(None);
        }
    }

    // Local -> Output
    fn submit_source_data(&mut self, data: &[u8]) -> Result<usize> {
        println!("Enter submit_source_data");
        // Create a DataWindow to view the data
        let limit = data.len() - (self.config.blocksize * self.config.seq_matches);
        let mut data = DataWindow::new(self.config.blocksize, limit, data);

        /*
        if self.offset > 0 {
            println!("Advancing by {} bytes!", self.skip_bytes);
            data.advance_n_bytes(self.skip_bytes)?;
        } else {
            self.next_match = None;
        }

        if self.offset == 0 || self.skip_bytes > 0 {
            self.rsums[0] = Rsum::calculate(data.get_cur_block());
            if self.config.seq_matches > 1 {
                self.rsums[1] = Rsum::calculate(data.get_nth_block(1).unwrap());
            }
        }
        */
        /////////////
        self.rsums[0] = Rsum::calculate(data.get_cur_block());
        if self.config.seq_matches > 1 {
            self.rsums[1] = Rsum::calculate(data.get_nth_block(1).unwrap());
        }
        /////////////
        
        self.skip_bytes = 0;

        let mut got_blocks = 0;

        // Search through until we get a block hit
        loop {
            println!("Considering block: {:#?}", data.get_cur_block());
            let blocks_found = self.check_block_match(data.get_cur_block())?;
            if let Some(b) = blocks_found {
                self.write_blocks(&b, data.get_cur_block())?;
                got_blocks += b.len();
                // TODO make sure next match doesn't go over
                //self.next_match = Some(b.iter().min().unwrap()+1);
                println!("Matched {} blocks!", b.len());

                let result = if self.config.seq_matches == 1 {
                    println!("Incrementing by 1 block");
                    data.advance_n_blocks(1)
                } else {
                    println!("Incrementing by 2 blocks");
                    data.advance_n_blocks(2)
                };

                if result.is_err() {
                    println!("Reached limit, exiting! {:#?}", result);
                    break;
                }

                self.rsums[0] = if self.config.seq_matches > 1 && self.config.seq_matches == 1 {
                    self.rsums[1]
                } else {
                    Rsum::calculate(data.get_cur_block())
                };

                if self.config.seq_matches > 1 {
                    self.rsums[1] = Rsum::calculate(data.get_nth_block(1).unwrap());
                }
            } else {
                println!("No match!");
                // We didn't match any data, advance the window by one byte and update the
                // rolling checksum.
                let nc = if let Ok(next_block) = data.get_nth_block(1) {
                    next_block[0]
                } else {
                    println!("Hit limit, exiting!");
                    break;
                };
                let oc = data.get_cur_block()[0];

                println!("Updating Rsums!");
                self.rsums[0].update(oc, nc, self.config.blocksize as u8);
                if self.config.seq_matches > 1 {
                    let nnc = if let Ok(next_next_block) = data.get_nth_block(2) {
                        next_next_block[0]
                    } else {
                        println!("Hit limit, exiting!");
                        break;
                    };
                    self.rsums[1].update(nc, nnc, self.config.blocksize as u8);
                }

                println!("Advancing by one byte!");
                if data.advance_byte().is_err() {
                    println!("Hit limit, exiting!");
                    break;
                }

                dbg!(Rsum::calculate(data.get_cur_block()));
                dbg!(self.rsums[0]);
                assert!(self.rsums[0] == Rsum::calculate(data.get_cur_block()));
            }
        }

        self.skip_bytes = data.get_remainder();
        dbg!(self.skip_bytes);
        return Ok(got_blocks);
    }

    // Remote -> Output
    fn submit_remote_block(&mut self, id: ZBlockId, data: &[u8]) -> Result<()> {
        //assert!(data.len() == ((start - end + 1) * self.config.blocksize));
        let checksum = PartialChecksum {
            value: MD4Digest::calculate(data),
            length: self.config.checksum_bytes,
        };
        dbg!(checksum);

        if checksum == self.blockmap.blocklist[id].checksum {
            // Write out the good blocks that we did get
            self.write_blocks(&vec![id], data)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rcksum_sanity() {
        let block_1 = ZBlock {
            rsum: Rsum::calculate(&[1; 16]), //(16, 136)
            checksum: PartialChecksum {
                value: MD4Digest::calculate(&[1; 16]).into(),
                length: 5,
            }
        };

        let block_2 = ZBlock {
            rsum: Rsum::calculate(&[2; 16]), //(32, 2)
            checksum: PartialChecksum {
                value: MD4Digest::calculate(&[2; 16]).into(),
                length: 5,
            }
        };
        let block_3 = ZBlock {
            rsum: Rsum::calculate(&[3; 16]), //(48, 168)
            checksum: PartialChecksum {
                value: MD4Digest::calculate(&[3; 16]).into(),
                length: 5,
            }
        };

        let block_list = vec![
            block_1,
            block_2,
            block_2,
            block_2,
            block_1,
            block_3,
            block_3,
            block_3,
            block_3,
            block_1,
        ];

        let config = Config {
            seq_matches: 1,
            checksum_bytes: 5,
            blocksize: 16,
        };
        let mut client = Context::new(config, 30, &Path::new("./myout")).unwrap();

        for i in 0..block_list.len() {
            client.blockmap.insert(i, block_list[i]);
        }

        // Try some incorrect blocks
        //client.submit_source_data(&[99; 16]).unwrap();
        //client.submit_remote_block(0, &[99; 16]).unwrap();
        //client.submit_remote_block(7, &[99; 16]).unwrap();

        // Add correct blocks
        //client.submit_source_data(&[1; 16]).unwrap();
        //client.submit_source_data(&[2; 16]).unwrap();
        //client.submit_source_data(&[3; 16]).unwrap();
        //client.submit_remote_block(5, &[3; 16]).unwrap();
        let mut concat_vec = Vec::new();
        concat_vec.push(9);
        concat_vec.push(9);
        
        for _i in 0..16 {
            concat_vec.push(1);
        }
        concat_vec.push(0);
        concat_vec.push(2);
        //concat_vec.push(0);
        for _i in 0..16 {
            concat_vec.push(2);
        }
        for _i in 0..16 {
            concat_vec.push(3);
        }
        
        client.submit_source_data(&concat_vec).unwrap();
    }
}
