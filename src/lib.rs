use std::{
    io::ErrorKind,
    path::PathBuf,
    sync::atomic::{AtomicU64, AtomicUsize},
};
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, ReadHalf, WriteHalf},
};

/// Potential Wishlist:
/// 1. Way to seek through the log somehow at an an arbitrary "index". Right now theres no concept
///    of an index but it might be useful when passing entries to a follower to go back to an
///    arbitrary index
/// 2. (Probably better than 1) Ability to seek backwards through the log somehow. When a follower
///    is behind it will need past committed entries and to do that you have to go backwards
///    through the log.
///    What this requires though is some fixed width to go back to the previous entry. The way I'm
///    currently allowing any width of an entry to exist makes this impossible.
///    But, if I make the fixed width u32 size that's insanely large and if I make it u16 it seems
///    pretty tiny. So the best option seems to be making it u16 sized and letting multiple records
///    make up one entry.
/// Plans:
/// This is inteded to be a WAL used by a distributed consensus algorithm (Raft, Viewstamped
/// repcliation, Paxos). I say that having only ever used Raft but I believe the needs of a WAL (or
/// just Log) used by one of any of these algorithms will be similar.
///
/// The following assumptions will be made in order to produce an efficient WAL for these
/// algorithms:
/// 1. Committed values will not be overwritten.
/// 2. Only committed values need to be persisted and recoverable on crashes.
/// 3. Seeking an arbitrary index in the log must be possible.
///
/// To achieve this the WAL will have the following properties. There will be a set of records in
/// the "buffer" that are not yet persisted to disk. Values can be taken from this buffer and
/// written to disk in batches or one at a time. (TODO TODO TODO: Confirm if the following sentence
/// is true) This is done by setting a commit index. (END TODO TODO TODO).  
/// One WAL record will have a maximum size of u16. WAL records will be grouped into blocks of
/// 32kb [Footnote 1].
///
/// The reading API will allow:
/// 1. Reading a block at index i
/// 2. Reading the current tail of the WAL
///
/// TODO TODO TODO: Need to understand what the effect of writing at on of heartbeat logs to disk
/// will be. They'll be tiny and I'm not sure they all need to be persitsed to disk? Or at least
/// have less of a requirement ot be persisted on committment maybe they can be grouped together
/// into bigger writes. Size wise I don't think its a big deal but it feels kinda wasteful
///
/// [Footnote 1] Often a leader will have to share recently committed values with a follower. There
/// isn't a hard cutoff for how far back this will need to go but it will often be "recent". By
/// having block sizes we can go back to "recent" records to quickly seek for the value we need.

/// TODO TODO TODO: Write tests exercising different cases the WAL would operate under.

pub enum WALError {
    // Indicates end of WAL
    // TODO: Determine if I like this patter, its how reading EOF works
    End,
    // Actual failures
    PayloadTooLarge(String),
    WriteAllFailure(io::Error),
    FlushFailure(io::Error),
    ReadExactFailure(io::Error),
}

const BLOCK_SIZE: usize = 32_000;
const MAX_TOTAL_RECORD_SIZE: usize = 128_000;
// The subtraced values come from the pieces of the WAL Record format
const MAX_WAL_RECORD_SIZE: u16 = (BLOCK_SIZE as u16) - 4 - 2 - 1;

pub struct WAL {
    /// Path to the WAL file
    path: PathBuf,
    /// Buffer of values yet to be committed
    buffer: Vec<Record>,
    /// Writer that will append to the end of the WAL
    writer: BufWriter<WriteHalf<File>>,
    /// Reader that will read blocks from the WAL
    reader: BufReader<ReadHalf<File>>,
    /// Number of records in the WAL (including in the buffer)
    /// This is full records. A Record that is broken up still only counts as 1 here.
    num_records: AtomicUsize,
    /// Number of full blocks in the WAL
    num_blocks: AtomicU64,
}

pub struct WALBlock {
    /// Block index in the WAL
    index: u64,
    /// Reader over the given block
    reader: BufReader<[u8; BLOCK_SIZE]>,
    /// Underlying bytes in the reader (TODO TODO TODO: This may not be necessary)
    bytes: [u8; BLOCK_SIZE],
    /// Length of the given block (None if it's a full block)
    len: Option<usize>,
}


/// WAL Record Format
/// +---------+-----------+-----------+--- ... ---+
/// |CRC (4B) | Size (2B) | Type (1B) | Payload   |
/// +---------+-----------+-----------+--- ... ---+
/// CRC - CRC on the payload bytes
/// Size - Size of the payload in bytes
/// Type - Type of the record
///        (Start, Middle, End)
/// Payload - Contents of the record

struct WALRecord {
    crc: [u8; 4],
    size: u16,
    record_type: u8,
    payload: Box<[u8]>,
}

enum RecordType {
    Full = 1,
    Start = 2,
    Middle = 3,
    End = 4,
}

pub struct Record {
    size: u16,
    payload: Box<[u8]>
}

impl Record {
    fn new(bytes: Box<[u8]>) -> Result<Record, String> {
        let len = bytes.as_ref().len();
        if len > MAX_TOTAL_RECORD_SIZE {
           return Err(format!(
                   "provided payload ({:?}) larger than maximum record size {:?}", 
                   len, 
                   MAX_TOTAL_RECORD_SIZE
            ));
        }
        Ok(Record {
            size: len as u16, 
            payload: bytes
        })
    }
}

enum ConvertedRecord {
    Single([WALRecord; 1]),
    Double([WALRecord; 2]),
    Triple([WALRecord; 3]),
    Quadruple([WALRecord; 4])
}


/// TODO TODO TODO: I want to write from Record to ConvertedRecord but it will require calling
/// async functions. Is there a way to accomplish this or will I just have to make my own trait?  

/// Converts a single Record into up to three WALRecords depending on the provided size
impl From<Record> for ConvertedRecord {
    fn from(value: Record) -> Self {
        if value.size < MAX_WAL_RECORD_SIZE {
            let single_record = WALRecord {
                crc: calc_crc(&value.payload).await,
                size: value.size,
                record_type: RecordType::Full,
                payload: value.payload,
            };
            ConvertedRecord::Single([single_record])
        } else if value.size < 2 * MAX_WAL_RECORD_SIZE {
            ConvertedRecord::Double([WALRecord {}, WALRecord {}])
        } else if value.size < 3 * MAX_WAL_RECORD_SIZE {
            ConvertedRecord::Triple([WALRecord {}, WALRecord {}, WALRecord {}])
        } else {
            ConvertedRecord::Quadruple([WALRecord {}, WALRecord {}, WALRecord {}, WALRecord {}])
        }
    }
}

// TODO TODO TODO: Pretty much all the read and write stuff needs to be rewritten to handle the new
// paradigm of splitting the WAL into blocks.
impl WAL {
    pub async fn new(path: PathBuf) -> io::Result<WAL> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create_new(true)
            .open(&path)
            .await?;

        Ok(Self::to_wal(path, file).await)
    }

    pub async fn from_path(path: PathBuf) -> io::Result<WAL> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)
            .await?;

        Ok(Self::to_wal(path, file).await)
    }

    async fn to_wal(path: PathBuf, file: File) -> WAL {
        let (read_half, write_half) = io::split(file);
        let writer = BufWriter::new(write_half);
        let reader = BufReader::new(read_half);
        let buffer = Vec::with_capacity(20);
        let num_records = AtomicUsize::new(0);
        let num_blocks = AtomicU64::new(0);

        WAL {
            path,
            writer,
            reader,
            buffer,
            num_records,
            num_blocks,
        }
    }

    pub async write(&mut self, payload: &[u8]) -> Result<(), WALError> {

    }

    /// Cancellation Safety: Right now this function is not cancellation safe because
    /// any call to `write_all` is not cancellation safe. TODO: I THINK what this means is that
    /// if any higher level function that eventually calls this function is cancelled before
    /// completion the WAL could be corrupted by a partial write. I don't have a good answer for
    /// how to fix that right now so I'm leaving this comment
    pub async fn write_REMOVE_THIS_FUNC(&mut self, payload: &[u8]) -> Result<(), WALError> {
        let crc_bytes: &[u8; 4] = calc_crc(payload).await;
        let size = payload.len();
        let size_bytes: &[u8; 4] = &TryInto::<u32>::try_into(size)
            .map_err(|_| WALError::PayloadTooLarge(format!("payload too large: {:?}", size)))?
            .to_le_bytes();

        self.writer
            .write_all(crc_bytes)
            .await
            .map_err(|e| WALError::WriteAllFailure(e))?;
        self.writer
            .write_all(size_bytes)
            .await
            .map_err(|e| WALError::WriteAllFailure(e))?;
        self.writer
            .write_all(payload)
            .await
            .map_err(|e| WALError::WriteAllFailure(e))?;

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), WALError> {
        self.writer
            .flush()
            .await
            .map_err(|e| WALError::FlushFailure(e))
    }

    pub async fn read_block(&mut self) -> Result<Option<WALRecord>, WALError> {
        let mut crc = [0; 4];
        let initial_read = self.reader.read_exact(&mut crc).await;
        match initial_read {
            // Tried to read past the end of the WAL
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Err(WALError::End),
            // Encountered an error reading from the WAL
            Err(err) => return Err(WALError::ReadExactFailure(err)),
            Ok(_) => {}
        };

        let mut size_bytes = [0; 4];
        self.reader
            .read_exact(&mut size_bytes)
            .await
            .map_err(|e| WALError::ReadExactFailure(e))?;
        let size = u32::from_le_bytes(size_bytes);

        let payload_len = size.try_into().map_err(|_| {
            WALError::PayloadTooLarge(format!("reading payload larger than allowable: {:?}", size))
        })?;
        let mut payload_bytes = vec![0; payload_len];
        self.reader
            .read_exact(&mut payload_bytes)
            .await
            .map_err(|e| WALError::ReadExactFailure(e))?;
        let payload = Box::from(payload_bytes.as_slice());

        Ok(Some(WALRecord { crc, size, payload }))
    }
}

async fn calc_crc(_payload: &[u8]) -> &[u8; 4] {
    return &[0, 0, 0, 0];
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
