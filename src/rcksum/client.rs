use std::fs::File;
use std::io::{Write, Seek, SeekFrom};
use std::path::Path;
use std::convert::TryInto;
use crate::error::*;
use super::types::*;
use super::map::*;

struct DataWindow<'a> {
    pos: usize,
    blocksize: usize,
    limit: usize, // Exclusive bound
    data: &'a [u8],
}

impl<'a> DataWindow<'a> {
    pub fn new(blocksize: usize, limit: usize, data: &'a [u8]) -> Self {
        assert!(blocksize <= data.len());
        assert!(limit <= data.len());
        DataWindow {
            pos: 0,
            blocksize,
            limit,
            data,
        }
    }

    pub fn advance_byte(&mut self) -> Result<()> {
        if self.pos + 1 >= self.limit {
            Err(Error::DataOutOfBounds {
                position: self.pos + 1,
                limit: self.limit
            })?;
        }
        self.pos += 1;
        Ok(())
    }

    pub fn advance_block(&mut self) -> Result<()> {
        if self.pos + self.blocksize >= self.limit {
            Err(Error::DataOutOfBounds {
                position: self.pos + self.blocksize,
                limit: self.limit
            })?;
        }
        self.pos += self.blocksize;
        Ok(())
    }

    pub fn advance_n_bytes(&mut self, n: usize) -> Result<()> {
        if self.pos + n >= self.limit {
            Err(Error::DataOutOfBounds {
                position: self.pos + n,
                limit: self.limit
            })?;
        }
        self.pos += n;
        Ok(())
    }

    pub fn get_cur_block(&mut self) -> &[u8] {
        &self.data[self.pos..self.pos+self.blocksize]
    }

    pub fn get_nth_block(&mut self, n: usize) -> Result<&[u8]> {
        if self.pos + n*self.blocksize >= self.limit {
            Err(Error::DataOutOfBounds {
                position: self.pos + n*self.blocksize,
                limit: self.limit
            })?;
        }
        Ok(&self.data[self.pos+self.blocksize..self.pos+n*self.blocksize])
    }

    pub fn get_remainder(&self) -> usize {
        self.limit - self.pos
    }

    pub fn is_at_limit(&self) -> bool {
        self.pos == self.limit
    }
}

#[derive(Copy, Clone)]
pub struct Config {
    seq_matches: usize,
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
        // Calculate bitshift for blocksize
        // TODO
        let mut file = File::create(output_path)?;
        for i in 0..num_blocks * config.blocksize {
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
            // TODO fixme
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
        // Create a DataWindow to view the data
        let limit = data.len() - (self.config.blocksize * self.config.seq_matches);
        let mut data = DataWindow::new(self.config.blocksize, limit, data);

        if self.offset > 0 {
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
        self.skip_bytes = 0;

        let mut got_blocks = 0;

        // Search through until we get a block hit
        'outer: loop {
            let mut blocks_matched = 0;

            /*
            if self.config.seq_matches > 1 {
                if let Some(e) = self.next_match {
                    let blocks_found = self.check_block_match(data.get_cur_block())?;
                    if let Some(b) = blocks_found {
                        self.write_blocks(&b, data.get_cur_block())?;
                        blocks_matched = 1;
                        got_blocks += b.len();
                    }
                }
            }
            */

            // Advance byte-by-byte through the data looking for a hit
            while blocks_matched == 0 {
                let blocks_found = self.check_block_match(data.get_cur_block())?;
                if let Some(b) = blocks_found {
                    self.write_blocks(&b, data.get_cur_block())?;
                    blocks_matched = self.config.seq_matches;
                    got_blocks += b.len();
                    self.next_match = Some(b.iter().min().unwrap()+1);
                    // TODO make sure next match doesn't go over
                } else {
                    // We didn't match any data, advance the window by one byte and update the
                    // rolling checksum.
                    let nc = if let Ok(next_block) = data.get_nth_block(1) {
                        next_block[0]
                    } else {
                        break 'outer;
                    };
                    let oc = data.get_cur_block()[0];
                    self.rsums[0].update(oc, nc, self.blockshift);
                    if self.config.seq_matches > 1 {
                        let nnc = if let Ok(next_next_block) = data.get_nth_block(2) {
                            next_next_block[0]
                        } else {
                            break 'outer;
                        };
                        self.rsums[1].update(nc, nnc, self.blockshift);
                    }
                    if data.advance_byte().is_err() {
                        break 'outer;
                    }
                }
            }

            if blocks_matched > 0 {
                if data.advance_block().is_err() {
                    break 'outer;
                }
                if blocks_matched > 1 {
                    if data.advance_block().is_err() {
                        break 'outer;
                    }
                }
            } // TODO: on advance block error, return

            if self.config.seq_matches > 1 && blocks_matched == 1 {
                self.rsums[0] = self.rsums[1];
            } else {
                self.rsums[0] = Rsum::calculate(data.get_cur_block());
            }

            if self.config.seq_matches > 1 {
                self.rsums[1] = Rsum::calculate(data.get_nth_block(1).unwrap());
            }
        }

        self.skip_bytes = data.get_remainder();
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
        let block_list = vec![
            ZBlock {
                rsum: Rsum(32, 254),
                checksum: PartialChecksum {
                    value: [39, 240, 238, 55, 4, 210, 113, 29, 186, 83, 85, 226, 140, 15, 15, 102].into(),
                    length: 5,
                }
            },
            ZBlock {
                rsum: Rsum(16, 16),
                checksum: PartialChecksum {
                    value: [147, 172, 48, 114, 97, 52, 72, 229, 120, 97, 181, 5, 207, 11, 54, 128].into(),
                    length: 5,
                }
            },
            ZBlock {
                rsum: Rsum(16, 16),
                checksum: PartialChecksum {
                    value: [147, 172, 48, 114, 97, 52, 72, 229, 120, 97, 181, 5, 207, 11, 54, 128].into(),
                    length: 5,
                }
            },
            ZBlock {
                rsum: Rsum(16, 16),
                checksum: PartialChecksum {
                    value: [147, 172, 48, 114, 97, 52, 72, 229, 120, 97, 181, 5, 207, 11, 54, 128].into(),
                    length: 5,
                }
            },
            ZBlock {
                rsum: Rsum(32, 254),
                checksum: PartialChecksum {
                    value: [39, 240, 238, 55, 4, 210, 113, 29, 186, 83, 85, 226, 140, 15, 15, 102].into(),
                    length: 5,
                }
            },
            ZBlock {
                rsum: Rsum(48, 78),
                checksum: PartialChecksum {
                    value: [4, 181, 10, 196, 178, 153, 72, 158, 149, 46, 208, 24, 41, 24, 145, 14].into(),
                    length: 5,
                }
            },
        ];

        let config = Config {
            seq_matches: 0,
            checksum_bytes: 5,
            blocksize: 16,
        };
        let mut client = Context::new(config, 30, &Path::new("./myout")).unwrap();

        for i in 0..block_list.len() {
            client.blockmap.insert(i, block_list[i]);
        }

        // Try some incorrect blocks
        client.submit_source_data(&[99; 16]).unwrap();
        client.submit_remote_block(0, &[99; 16]).unwrap();
        client.submit_remote_block(7, &[99; 16]).unwrap();

        // Add correct blocks
        client.submit_source_data(&[2; 16]).unwrap();
        client.submit_source_data(&[1; 16]).unwrap();
        client.submit_remote_block(5, &[3; 16]).unwrap();
    }
}
