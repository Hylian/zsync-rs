use crate::error::*;

pub struct DataWindow<'a> {
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
        if self.pos + 1 > self.limit {
            Err(Error::DataOutOfBounds {
                position: self.pos + 1,
                limit: self.limit
            })?;
        }
        self.pos += 1;
        Ok(())
    }

    pub fn advance_n_blocks(&mut self, num_blocks: usize) -> Result<()> {
        let newpos = self.pos + self.blocksize * num_blocks;
        if newpos > self.limit {
            Err(Error::DataOutOfBounds {
                position: newpos,
                limit: self.limit
            })?;
        }
        self.pos = newpos;
        Ok(())
    }

    pub fn advance_n_bytes(&mut self, n: usize) -> Result<()> {
        if self.pos + n > self.limit {
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
        let newpos = self.pos + n*self.blocksize ;
        if newpos > self.limit {
            Err(Error::DataOutOfBounds {
                position: newpos,
                limit: self.limit
            })?;
        }
        dbg!(self.data.len());
        dbg!(self.pos);
        dbg!(newpos);
        dbg!(newpos+self.blocksize);
        Ok(&self.data[newpos..newpos+self.blocksize])
    }

    pub fn get_remainder(&self) -> usize {
        self.limit - self.pos
    }

    pub fn is_at_limit(&self) -> bool {
        self.pos == self.limit
    }
}
