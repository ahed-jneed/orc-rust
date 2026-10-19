// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

pub mod metadata;

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use bytes::{Buf, Bytes};

/// Largest buffer reserved up front for a read whose source cannot report how many bytes
/// it still holds. Longer reads grow past this as bytes actually arrive.
const MAX_UNVERIFIED_PREALLOCATION: u64 = 8 * 1024 * 1024;

/// How much to reserve for reading `length` bytes when `available` bytes remain in the
/// source, if known. The requested length comes from file metadata so it is never trusted
/// beyond what the source can hold.
fn initial_capacity(length: u64, available: Option<u64>) -> usize {
    let bound = available.unwrap_or(MAX_UNVERIFIED_PREALLOCATION);
    usize::try_from(length.min(bound)).unwrap_or(0)
}

fn read_exact_bounded(
    reader: impl Read,
    length: u64,
    available: Option<u64>,
) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(initial_capacity(length, available));
    reader.take(length).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    Ok(bytes)
}

/// Primary source used for reading required bytes for operations.
#[allow(clippy::len_without_is_empty)]
pub trait ChunkReader {
    type T: Read;

    /// Get total length of bytes. Useful for parsing the metadata located at
    /// the end of the file.
    // TODO: this is only used for file tail, so replace with load_metadata?
    fn len(&self) -> u64;

    /// Get a reader starting at a specific offset.
    fn get_read(&self, offset_from_start: u64) -> std::io::Result<Self::T>;

    /// Read bytes from an offset with specific length.
    fn get_bytes(&self, offset_from_start: u64, length: u64) -> std::io::Result<Bytes> {
        let available = self.len().checked_sub(offset_from_start);
        let reader = self.get_read(offset_from_start)?;
        Ok(read_exact_bounded(reader, length, available)?.into())
    }
}

impl ChunkReader for File {
    type T = BufReader<File>;

    fn len(&self) -> u64 {
        self.metadata().map(|m| m.len()).unwrap_or(0u64)
    }

    /// Care needs to be taken when using this simultaneously as underlying
    /// file descriptor is the same and will be affected by other invocations.
    ///
    /// See [`File::try_clone()`] for more details.
    fn get_read(&self, offset_from_start: u64) -> std::io::Result<Self::T> {
        let mut reader = self.try_clone()?;
        reader.seek(SeekFrom::Start(offset_from_start))?;
        Ok(BufReader::new(self.try_clone()?))
    }
}

impl ChunkReader for Bytes {
    type T = bytes::buf::Reader<Bytes>;

    fn len(&self) -> u64 {
        self.len() as u64
    }

    fn get_read(&self, offset_from_start: u64) -> std::io::Result<Self::T> {
        let offset = usize::try_from(offset_from_start)
            .ok()
            .filter(|offset| *offset <= self.len())
            .ok_or(std::io::ErrorKind::UnexpectedEof)?;
        Ok(self.slice(offset..).reader())
    }

    fn get_bytes(&self, offset_from_start: u64, length: u64) -> std::io::Result<Bytes> {
        let range = usize::try_from(offset_from_start)
            .ok()
            .zip(usize::try_from(length).ok())
            .and_then(|(offset, length)| Some(offset..offset.checked_add(length)?))
            .filter(|range| range.end <= self.len())
            .ok_or(std::io::ErrorKind::UnexpectedEof)?;
        Ok(self.slice(range))
    }
}

#[cfg(feature = "async")]
mod async_chunk_reader {
    use super::*;

    use futures_util::future::BoxFuture;
    use futures_util::FutureExt;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

    #[allow(clippy::len_without_is_empty)]
    pub trait AsyncChunkReader: Send {
        // TODO: this is only used for file tail, so replace with load_metadata?
        fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>>;

        fn get_bytes(
            &mut self,
            offset_from_start: u64,
            length: u64,
        ) -> BoxFuture<'_, std::io::Result<Bytes>>;
    }

    impl<T: AsyncRead + AsyncSeek + Unpin + Send> AsyncChunkReader for T {
        fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
            async move { self.seek(SeekFrom::End(0)).await }.boxed()
        }

        fn get_bytes(
            &mut self,
            offset_from_start: u64,
            length: u64,
        ) -> BoxFuture<'_, std::io::Result<Bytes>> {
            async move {
                let available = self
                    .seek(SeekFrom::End(0))
                    .await?
                    .checked_sub(offset_from_start);
                self.seek(SeekFrom::Start(offset_from_start)).await?;
                let capacity = initial_capacity(length, available);
                let mut buffer = Vec::with_capacity(capacity);
                (&mut *self).take(length).read_to_end(&mut buffer).await?;
                if buffer.len() as u64 != length {
                    return Err(std::io::ErrorKind::UnexpectedEof.into());
                }
                Ok(buffer.into())
            }
            .boxed()
        }
    }

    impl AsyncChunkReader for Box<dyn AsyncChunkReader> {
        fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
            self.as_mut().len()
        }

        fn get_bytes(
            &mut self,
            offset_from_start: u64,
            length: u64,
        ) -> BoxFuture<'_, std::io::Result<Bytes>> {
            self.as_mut().get_bytes(offset_from_start, length)
        }
    }
}

#[cfg(feature = "async")]
pub use async_chunk_reader::AsyncChunkReader;

#[cfg(all(feature = "async", feature = "opendal"))]
mod async_opendal_reader {
    use crate::reader::AsyncChunkReader;
    use bytes::Bytes;
    use futures_util::future::BoxFuture;
    use opendal::Operator;
    use std::sync::Arc;

    /// AsyncOpendalReader provides native support for [`opendal`]
    ///
    /// ```
    /// use opendal::Operator;
    /// use std::io::Result;
    /// use orc_rust::reader::AsyncOpendalReader;
    /// use orc_rust::reader::AsyncChunkReader;
    /// use opendal::services::MemoryConfig;
    ///
    /// # async fn test() -> Result<()> {
    /// let op = Operator::from_config(MemoryConfig::default())?.finish();
    /// op.write("test", "Hello, world!").await?;
    ///
    /// let mut reader = AsyncOpendalReader::new(op, "test");
    /// let len = reader.len().await?;
    /// let data = reader.get_bytes(0, len).await?;
    /// #    Ok(())
    /// # }
    /// ```
    pub struct AsyncOpendalReader {
        op: Operator,
        path: Arc<String>,
    }

    impl AsyncOpendalReader {
        /// Create a new async opendal reader.
        pub fn new(op: Operator, path: &str) -> Self {
            Self {
                op,
                path: Arc::new(path.to_string()),
            }
        }
    }

    impl AsyncChunkReader for AsyncOpendalReader {
        fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
            let path = self.path.clone();
            Box::pin(async move {
                let meta = self.op.stat(&path).await?;
                Ok(meta.content_length())
            })
        }

        fn get_bytes(
            &mut self,
            offset_from_start: u64,
            length: u64,
        ) -> BoxFuture<'_, std::io::Result<Bytes>> {
            let path = self.path.clone();

            Box::pin(async move {
                let reader = self
                    .op
                    .read_with(&path)
                    .range(offset_from_start..offset_from_start + length)
                    .await?;
                Ok(reader.to_bytes())
            })
        }
    }
}

#[cfg(all(feature = "async", feature = "opendal"))]
pub use async_opendal_reader::AsyncOpendalReader;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_get_read_offset_past_end() {
        let bytes = Bytes::from_static(&[1, 2, 3]);
        assert!(bytes.get_read(4).is_err());
        assert!(bytes.get_bytes(4, 1).is_err());
    }

    #[test]
    fn test_get_bytes_is_zero_copy_for_bytes() {
        let bytes = Bytes::from(vec![7; 1024]);
        let slice = bytes.get_bytes(100, 200).unwrap();
        assert_eq!(slice.len(), 200);
        assert_eq!(slice.as_ptr(), bytes[100..].as_ptr());
        assert!(bytes.get_bytes(900, 125).is_err());
        assert!(bytes.get_bytes(u64::MAX, 2).is_err());
    }

    #[test]
    fn test_read_exact_bounded_allocates_exact_size() {
        let length = 64 * 1024 * 1024;
        let data = vec![1_u8; length];
        let read = read_exact_bounded(&data[..], length as u64, Some(length as u64)).unwrap();
        assert_eq!(read.len(), length);
        assert_eq!(read.capacity(), length);
    }

    #[test]
    fn test_read_exact_bounded_refuses_hostile_length() {
        let data = [1, 2, 3];
        let length = u64::MAX / 2;
        assert!(initial_capacity(length, Some(3)) <= 3);
        assert!(initial_capacity(length, None) as u64 <= MAX_UNVERIFIED_PREALLOCATION);
        assert!(read_exact_bounded(&data[..], length, Some(3)).is_err());
        assert!(read_exact_bounded(&data[..], length, None).is_err());
    }

    #[test]
    fn test_get_bytes_length_beyond_input() {
        let bytes = Bytes::from_static(&[1, 2, 3]);
        assert!(bytes.get_bytes(1, u64::MAX / 2).is_err());
        assert_eq!(bytes.get_bytes(1, 2).unwrap().as_ref(), &[2, 3]);
    }
}
