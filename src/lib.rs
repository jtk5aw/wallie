use std::{io::ErrorKind, path::PathBuf};
use tokio::{
    fs::{File, OpenOptions},
    io::{self, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, ReadHalf, WriteHalf},
    task,
};

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

pub struct WAL {
    path: PathBuf,
    writer: BufWriter<WriteHalf<File>>,
    reader: BufReader<ReadHalf<File>>,
}

/**
 * WAL Record Format
 * +---------+-----------+--- ... ---+
 * |CRC (4B) | Size (2B) | Payload   |
 * +---------+-----------+--- ... ---+
 * CRC - This is the CRC on the payload bytes
 * Size - This is the size of the payload in bytes
 * Paylaod - Contents of the record
 */
pub struct WALRecord {
    crc: [u8; 4],
    size: u32,
    payload: Box<[u8]>,
}

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

    /// TODO TODO TODO: Need to figure out if opening with append puts the reader at the end of the
    /// file too cause that is a problem. We want the reader to be at the beginning and the writer
    /// at the end.
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

        WAL {
            path,
            writer,
            reader,
        }
    }

    /// Cancellation Safety: Right now this function is not cancellation safe because
    /// any call to `write_all` is not cancellation safe. TODO: I THINK what this means is that
    /// if any higher level function that eventually calls this function is cancelled before
    /// completion the WAL could be corrupted by a partial write. I don't have a good answer for
    /// how to fix that right now so I'm leaving this comment
    pub async fn write(&mut self, payload: &[u8]) -> Result<(), WALError> {
        let crc_bytes = calc_crc(payload).await;
        let size = payload.len();
        let size_bytes = &TryInto::<u32>::try_into(size)
            .map_err(|err| WALError::PayloadTooLarge(format!("payload too large: {:?}", size)))?
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

    pub async fn read(&mut self) -> Result<Option<WALRecord>, WALError> {
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
