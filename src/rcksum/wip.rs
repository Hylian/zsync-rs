type ZBlockId = usize;

struct ZBlock {
    rsum: Rsum,
    checksum: PartialChecksum,
}

type ChecksumMap = HashMap<PartialChecksum, Vec<ZBlockId>>;

impl ChecksumMap {
    pub fn search(&self, checksum: PartialChecksum) -> Option<Vec<ZBlockId>> {
        self.get(checksum)
    }
}

struct ZBlockMap {
    rsum_map: HashMap<Rsum, ChecksumMap>,
    blocklist: Vec<ZBlock>,
}

impl ZBlockMap {
    pub fn new() -> Self {
        ZBlockMap {
            hashmap: HashMap::new(),
            blocklist: Vec::new(),
        }
    }

    pub fn search(&mut self, rsum: Rsum) -> Option<ChecksumMap> {
        self.rsum_map.get(rsum)
    }

    pub fn insert(&mut self, block_id: ZBlockId, block: ZBlock) -> Result<()> {
        self.blocklist[block_id] = block;

        if !self.rsum_map.contains_key(block.rsum) {
            self.rsum_map.insert(block.rsum, HashMap::new());
        }

        let mut checksum_map = self.rsum_map.get_mut(block.rsum);
        
        if !checksum_map.contains_key(block.checksum) {
            checksum_map.insert(block.checksum, Vec::new());
        }

        let mut block_list = checksum_map.get_mut(block.checksum);

        block_list.push(block_id);

        Ok(())
    }

    pub fn remove(&mut self, block_id: ZBlockId) -> Result<()> {
        let block = self.blocklist[block_id];
        let mut checksum_map = self.rsum_map.get_mut(block.rsum)?;
        let mut block_list = checksum_map.get_mut(block.checksum)?;
        block_list.remove_item(block_id);

        if block_list.is_empty() {
            checksum_map.remove(block.checksum).unwrap();
        }

        if checksum_map.is_empty() {
            rsum_map.remove(block.rsum).unwrap();
        }

        Ok(())
    }
}

struct Rsum(u8, u8);

impl Rsum {
    // Calculate the checksum of a block
    fn calculate(data: &[u8]) -> Self {
        let result = data.iter.fold((Rsum(0,0), data.len()), |acc, x| {
            acc.0.0 += x;
            acc.0.1 += acc.1 * x;
            acc.1 -= 1;
            acc
        });
        assert!(result.1 == 0);
        result.0
    }

    // Update the rolling checksum with the next byte
    fn update(self, old: u8, new: u8, blockshift: i32) -> Self {
        self.0 += new - old;
        self.1 += self.0 - (old << blockshift);
        self
    }
}

enum Checksum {
    MD4Digest([u8; 16]),
}

impl Checksum {
    fn calculate(data: &[u8]) -> Self {
        Checksum::MD4Digest([0; 16])
    }
}

type BlockId = i32;

struct DataWindow {
    pos: usize,
    blocksize: usize,
    limit: usize, // Exclusive bound
    data: &[u8],
}

impl DataWindow {
    pub fn new(blocksize: usize, limit: usize, data: &[u8]) -> Self {
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
        if (self.pos + self.blocksize >= self.limit) {
            return Err();
        }
        self.pos += 1;
        Ok(())
    }

    pub fn advance_block(&mut self) -> Result<()> {
        if (self.pos + 2*self.blocksize >= self.limit) {
            return Err();
        }
        self.pos += self.blocksize;
        Ok(())
    }

    pub fn advance_n(&mut self, n: usize) -> Result<()> {
        if (self.pos + n >= self.limit) {
            return Err();
        }
        self.pos += n;
        Ok(())
    }

    pub fn get_cur_block(&mut self) -> &[u8] {
        &self.data[self.pos..self.pos+self.blocksize]
    }

    pub fn get_nth_block(&mut self, n: usize) -> Result<&[u8]> {
        if (self.pos + n*self.blocksize >= self.limit) {
            return Err();
        }
        &self.data[self.pos+self.blocksize..self.pos+n*self.blocksize]
    }

    pub fn get_remainder(&self) -> usize {
        self.pos - self.limit
    }

    pub fn is_at_limit(&self) -> bool {
        self.pos == self.limit
    }
}

struct Config {
    seq_matches: i32,
}

struct Context {
    config: Config,

    rsums: [Rsum, 2],
    num_blocks: BlockId,
    block_shift: i32, // log2(blocksize)
    rsum_a_mask: u8,
    checksum_bytes: usize,

    min_bytes: u32, // precalculated blocksize * seq_matches

    blocksize: usize,

    skip_bytes: usize,
    offset: usize,
}

impl Context {
    fn check_block_match(&mut self, data: &[u8]) -> Result<(bool, Option<>)> {

    }

    // Local -> Output
    fn submit_source_data(&mut self, data: &[u8]) -> Result<usize> {
        // Create a DataWindow to view the data
        let limit = data.len() - (self.blocksize * self.seq_matches);
        let data = DataWindow::new(self.blocksize, limit, data);

        if self.offset > 0 {
            data.advance_n(self.skip_bytes)?;
        } else {
            self.next_match = None;
        }

        if (self.offset == 0) || self.skip_bytes > 0) {
            self.rsums[0] = Rsum::calculate(data.get_cur_block());
            if self.config.seq_matches > 1 {
                self.rsums[1] = Rsum::calculate(data.get_nth_block(1).unwrap());
            }
        }
        self.skip_bytes = 0;

        let mut got_blocks = 0;

        // Search through until we get a block hit
        loop {
            let mut blocks_matched = 0;

            if self.config.seq_matches > 1 {
                if let Some(e) = self.next_match {
                    let (num_matches, self.next_match) = self.check_data_block(data.get_cur_block())?;
                    if num_matches > 0 {
                        blocks_matched = 1;
                        got_blocks += num_matches;
                    }
                }
            }

            // Advance byte-by-byte through the data looking for a hit
            while blocks_matched == 0 {
                let (num_matches, self.next_match) = self.check_data_block(data.get_cur_block())?;
                if num_matches > 0 {
                    blocks_matched = self.config.seq_matches;
                    got_blocks += num_matches;
                } else {
                    // We didn't match any data, advance the window by one byte and update the
                    // rolling checksum.
                    let nc = data.get_nth_block(1)[0];
                    let oc = data.get_cur_block()[0];
                    self.rsums[0].update(data.get_cur_block()[0], oc, nc, self.blockshift);
                    if self.config.seq_matches > 1 {
                        let Nc = data.get_nth_block(2)[0];
                        self.rsums[1].update(data.get_cur_block()[0], oc, nc, self.blockshift);
                    }
                    data.advance_byte();
                }
            }

            if blocks_matched > 0 {
                data.advance_block();
                if blocks_matched > 1 {
                    data.advance_block();
                }
            } // TODO: on advance block error, return

            if self.config.seq_matches > 1 && blocks_matched == 1 {
                self.rsums[0] = self.rsums[1];
            } else {
                self.rsums[0] = Rsum::calculate(data.get_cur_block().unwrap());
            }

            if self.config.seq_matches > 1 {
                self.rsums[1] = Rsum::calculate(data.get_nth_block(1).unwrap());
            }
        }

        self.skip_bytes = data.get_remainder();
        return Ok(got_blocks);
    }

    // Remote -> Output
    fn submit_remote_blocks(&mut self, data: &[u8], start_block: BlockId, end_block: BlockId) -> Result<()> {
        assert!(data.len() == ((start - end + 1) * self.blocksize));

        for block in start_block..end_block {
            let checksum = Checksum::calculate();
            if checksum != self.block_hashes.checksum {
                if block > from {
                    // Write out the good blocks that we did get
                    write_blocks();
                    return Err(());
                }
            }
        }

        write_blocks();
        Ok(())
    }
}













// Hash Table
//  hashmask
//  rsum_hash ???
//  bithashmask
//  bithash
//  blockhashes: list of hash_entry's

struct HashTable {
    hashmask: usize,
    bithash: usize,
    bithashmask: usize,
}

struct HashEntry {
    //TODO: ptr to next entry with same rsum
    rsum: Rsum, // rolling Adler-style checksum
    checksum: MD4Digest,
}

