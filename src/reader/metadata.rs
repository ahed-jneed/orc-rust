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

//! Parse ORC file tail metadata structures from file.
//!
//! File tail structure:
//!
//! ```text
//! ------------------
//! |    Metadata    |
//! |                |
//! ------------------
//! |     Footer     |
//! |                |
//! ------------------
//! |  Postscript  |X|
//! ------------------
//! ```
//!
//! Where X is last byte in file indicating
//! Postscript length in bytes.
//!
//! Footer and Metadata lengths are encoded in Postscript.
//! Postscript is never compressed, Footer and Metadata
//! may be compressed depending Postscript config value.
//!
//! If they are compressed then their lengths indicate their
//! compressed lengths.

use bytes::{Bytes, BytesMut};
use prost::Message;
use snafu::{ensure, OptionExt, ResultExt};
use std::collections::HashMap;

use crate::compression::{
    read_decompressed_section_with_limit, Compression, MAX_COMPRESSION_BLOCK_SIZE,
    MAX_DECOMPRESSED_SECTION_SIZE,
};
use crate::error::{self, EmptyFileSnafu, OutOfSpecSnafu, Result};
use crate::proto::{self, CompressionKind, Footer, Metadata, PostScript};
use crate::schema::RootDataType;
use crate::statistics::ColumnStatistics;
use crate::stripe::StripeMetadata;
use crate::wire::{FOOTER, POSTSCRIPT};

use crate::reader::ChunkReader;

const DEFAULT_FOOTER_SIZE: u64 = 16 * 1024;

/// Length of the `ORC` magic that starts every file, before the first stripe.
const ORC_MAGIC_LENGTH: u64 = 3;

/// The file's metadata.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    compression: Option<Compression>,
    root_data_type: RootDataType,
    number_of_rows: u64,
    file_format_version: String,
    /// Statistics of columns across entire file
    column_statistics: Vec<ColumnStatistics>,
    stripes: Vec<StripeMetadata>,
    user_custom_metadata: HashMap<String, Vec<u8>>,
    /// The maximum number of rows in each index entry (default 10,000)
    row_index_stride: Option<u32>,
    writer: Option<u32>,
}

impl FileMetadata {
    fn from_proto(
        postscript: &proto::PostScript,
        footer: &proto::Footer,
        metadata: &proto::Metadata,
        stripes_end: u64,
    ) -> Result<Self> {
        validate_stripe_layout(footer, stripes_end)?;
        validate_statistics(&footer.types, &footer.statistics)?;
        for stripe_stats in &metadata.stripe_stats {
            validate_statistics(&footer.types, &stripe_stats.col_stats)?;
        }
        let compression =
            Compression::from_proto(postscript.compression(), postscript.compression_block_size);
        let root_data_type = RootDataType::from_proto(&footer.types)?;
        let number_of_rows = footer.number_of_rows();
        let column_statistics = footer
            .statistics
            .iter()
            .map(TryFrom::try_from)
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            metadata.stripe_stats.is_empty() || metadata.stripe_stats.len() == footer.stripes.len(),
            OutOfSpecSnafu {
                msg: "stripe stats length must equal the number of stripes"
            }
        );
        // TODO: confirm if this is valid
        let stripes = if metadata.stripe_stats.is_empty() {
            footer
                .stripes
                .iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>>>()?
        } else {
            footer
                .stripes
                .iter()
                .zip(metadata.stripe_stats.iter())
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>>>()?
        };
        let user_custom_metadata = footer
            .metadata
            .iter()
            .map(|kv| (kv.name().to_owned(), kv.value().to_vec()))
            .collect::<HashMap<_, _>>();

        let file_format_version = postscript
            .version
            .iter()
            .map(|v| v.to_string() + ".")
            .collect::<String>();
        let file_format_version = file_format_version
            .strip_suffix('.')
            .unwrap_or("")
            .to_string();

        Ok(Self {
            compression,
            root_data_type,
            number_of_rows,
            file_format_version,
            column_statistics,
            stripes,
            user_custom_metadata,
            row_index_stride: footer.row_index_stride,
            writer: footer.writer,
        })
    }

    pub fn number_of_rows(&self) -> u64 {
        self.number_of_rows
    }

    pub fn compression(&self) -> Option<Compression> {
        self.compression
    }

    pub fn root_data_type(&self) -> &RootDataType {
        &self.root_data_type
    }

    pub fn column_file_statistics(&self) -> &[ColumnStatistics] {
        &self.column_statistics
    }

    pub fn stripe_metadatas(&self) -> &[StripeMetadata] {
        &self.stripes
    }

    pub fn user_custom_metadata(&self) -> &HashMap<String, Vec<u8>> {
        &self.user_custom_metadata
    }

    /// Get the row index stride (rows per row group)
    ///
    /// Returns the number of rows per row group used for row-level indexes.
    /// Default is 10,000 according to ORC spec.
    ///
    /// If `None` is returned, it means row indexes are not enabled for this file.
    pub fn row_index_stride(&self) -> Option<usize> {
        self.row_index_stride.map(|s| s as usize)
    }

    pub fn writer(&self) -> Option<u32> {
        self.writer
    }

    pub fn file_format_version(&self) -> &str {
        &self.file_format_version
    }
}

pub fn read_metadata<R: ChunkReader>(reader: &mut R) -> Result<FileMetadata> {
    let file_len = reader.len();
    if file_len == 0 {
        return EmptyFileSnafu.fail();
    }

    // Initial read of the file tail
    // Use a default size for first read in hopes of capturing all sections with one read
    // At worst need two reads to get all necessary bytes
    let assume_footer_len = file_len.min(DEFAULT_FOOTER_SIZE);
    let mut tail_bytes = reader
        .get_bytes(file_len - assume_footer_len, assume_footer_len)
        .context(error::IoSnafu)?;

    // The final byte of the file contains the serialized length of the Postscript,
    // which must be less than 256 bytes.
    let postscript_len = tail_bytes[tail_bytes.len() - 1] as u64;
    tail_bytes.truncate(tail_bytes.len() - 1);

    if tail_bytes.len() < postscript_len as usize {
        return OutOfSpecSnafu {
            msg: "File too small for given postscript length",
        }
        .fail();
    }
    let postscript = PostScript::decode(&tail_bytes[tail_bytes.len() - postscript_len as usize..])
        .context(error::DecodeProtoSnafu)?;
    POSTSCRIPT
        .ensure_singular_fields_once(&tail_bytes[tail_bytes.len() - postscript_len as usize..])?;
    validate_postscript(&postscript)?;
    let compression =
        Compression::from_proto(postscript.compression(), postscript.compression_block_size);
    tail_bytes.truncate(tail_bytes.len() - postscript_len as usize);

    let footer_length = postscript.footer_length.context(error::OutOfSpecSnafu {
        msg: "Footer length is empty",
    })?;
    let metadata_length = postscript.metadata_length.unwrap_or(0);
    let sections_length =
        footer_length
            .checked_add(metadata_length)
            .context(error::OutOfSpecSnafu {
                msg: "Footer and metadata lengths overflow",
            })?;
    let tail_payload_length =
        file_len
            .checked_sub(1 + postscript_len)
            .context(error::OutOfSpecSnafu {
                msg: "Postscript extends past the end of the file",
            })?;
    ensure!(
        sections_length <= tail_payload_length,
        OutOfSpecSnafu {
            msg: "Footer and metadata extend past the end of the file"
        }
    );
    let stripes_end = tail_payload_length - sections_length;

    // Ensure we have enough bytes for Footer and Metadata
    let mut tail_bytes = if sections_length > tail_bytes.len() as u64 {
        // Need second read
        // -1 is the postscript length byte
        let offset = file_len - 1 - postscript_len - sections_length;
        let bytes_to_read = sections_length - tail_bytes.len() as u64;
        let prepend_bytes = reader
            .get_bytes(offset, bytes_to_read)
            .context(error::IoSnafu)?;
        let mut all_bytes = BytesMut::with_capacity(prepend_bytes.len() + tail_bytes.len());
        all_bytes.extend_from_slice(&prepend_bytes);
        all_bytes.extend_from_slice(&tail_bytes);
        all_bytes.into()
    } else {
        tail_bytes
    };

    let (footer, footer_size) = deserialize_footer(
        tail_bytes.slice(tail_bytes.len() - footer_length as usize..),
        compression,
        MAX_DECOMPRESSED_SECTION_SIZE,
    )?;
    tail_bytes.truncate(tail_bytes.len() - footer_length as usize);

    let remaining_limit = MAX_DECOMPRESSED_SECTION_SIZE
        .checked_sub(footer_size)
        .context(error::OutOfSpecSnafu {
            msg: "decompressed ORC footer and metadata exceed the shared limit",
        })?;
    let (metadata, _) = deserialize_footer_metadata(
        tail_bytes.slice(tail_bytes.len() - metadata_length as usize..),
        compression,
        remaining_limit,
    )?;

    FileMetadata::from_proto(&postscript, &footer, &metadata, stripes_end)
}

#[cfg(feature = "async")]
pub async fn read_metadata_async<R: super::AsyncChunkReader>(
    reader: &mut R,
) -> Result<FileMetadata> {
    let file_len = reader.len().await.context(error::IoSnafu)?;
    if file_len == 0 {
        return EmptyFileSnafu.fail();
    }

    // Initial read of the file tail
    // Use a default size for first read in hopes of capturing all sections with one read
    // At worst need two reads to get all necessary bytes
    let assume_footer_len = file_len.min(DEFAULT_FOOTER_SIZE);
    let mut tail_bytes = reader
        .get_bytes(file_len - assume_footer_len, assume_footer_len)
        .await
        .context(error::IoSnafu)?;

    // The final byte of the file contains the serialized length of the Postscript,
    // which must be less than 256 bytes.
    let postscript_len = tail_bytes[tail_bytes.len() - 1] as u64;
    tail_bytes.truncate(tail_bytes.len() - 1);

    if tail_bytes.len() < postscript_len as usize {
        return OutOfSpecSnafu {
            msg: "File too small for given postscript length",
        }
        .fail();
    }
    let postscript = PostScript::decode(&tail_bytes[tail_bytes.len() - postscript_len as usize..])
        .context(error::DecodeProtoSnafu)?;
    POSTSCRIPT
        .ensure_singular_fields_once(&tail_bytes[tail_bytes.len() - postscript_len as usize..])?;
    validate_postscript(&postscript)?;
    let compression =
        Compression::from_proto(postscript.compression(), postscript.compression_block_size);
    tail_bytes.truncate(tail_bytes.len() - postscript_len as usize);

    let footer_length = postscript.footer_length.context(error::OutOfSpecSnafu {
        msg: "Footer length is empty",
    })?;
    let metadata_length = postscript.metadata_length.unwrap_or(0);
    let sections_length =
        footer_length
            .checked_add(metadata_length)
            .context(error::OutOfSpecSnafu {
                msg: "Footer and metadata lengths overflow",
            })?;
    let tail_payload_length =
        file_len
            .checked_sub(1 + postscript_len)
            .context(error::OutOfSpecSnafu {
                msg: "Postscript extends past the end of the file",
            })?;
    ensure!(
        sections_length <= tail_payload_length,
        OutOfSpecSnafu {
            msg: "Footer and metadata extend past the end of the file"
        }
    );
    let stripes_end = tail_payload_length - sections_length;

    // Ensure we have enough bytes for Footer and Metadata
    let mut tail_bytes = if sections_length > tail_bytes.len() as u64 {
        // Need second read
        // -1 is the postscript length byte
        let offset = file_len - 1 - postscript_len - sections_length;
        let bytes_to_read = sections_length - tail_bytes.len() as u64;
        let prepend_bytes = reader
            .get_bytes(offset, bytes_to_read)
            .await
            .context(error::IoSnafu)?;
        let mut all_bytes = BytesMut::with_capacity(prepend_bytes.len() + tail_bytes.len());
        all_bytes.extend_from_slice(&prepend_bytes);
        all_bytes.extend_from_slice(&tail_bytes);
        all_bytes.into()
    } else {
        tail_bytes
    };

    let (footer, footer_size) = deserialize_footer(
        tail_bytes.slice(tail_bytes.len() - footer_length as usize..),
        compression,
        MAX_DECOMPRESSED_SECTION_SIZE,
    )?;
    tail_bytes.truncate(tail_bytes.len() - footer_length as usize);

    let remaining_limit = MAX_DECOMPRESSED_SECTION_SIZE
        .checked_sub(footer_size)
        .context(error::OutOfSpecSnafu {
            msg: "decompressed ORC footer and metadata exceed the shared limit",
        })?;
    let (metadata, _) = deserialize_footer_metadata(
        tail_bytes.slice(tail_bytes.len() - metadata_length as usize..),
        compression,
        remaining_limit,
    )?;

    FileMetadata::from_proto(&postscript, &footer, &metadata, stripes_end)
}

/// Refuses a postscript whose compression settings cannot describe a readable file.
fn validate_postscript(postscript: &PostScript) -> Result<()> {
    if let Some(kind) = postscript.compression {
        let kind = CompressionKind::try_from(kind)
            .ok()
            .context(OutOfSpecSnafu {
                msg: format!("Unknown compression kind {kind}"),
            })?;
        if kind != CompressionKind::None {
            if let Some(size) = postscript.compression_block_size {
                ensure!(
                    (1..MAX_COMPRESSION_BLOCK_SIZE).contains(&size),
                    OutOfSpecSnafu {
                        msg: format!(
                            "Compression block size {size} is outside 1..{MAX_COMPRESSION_BLOCK_SIZE}"
                        )
                    }
                );
            }
        }
    }
    Ok(())
}

/// Refuses stripes that do not lie, in order and without overlap, between the file's
/// leading magic and the start of the metadata section (`stripes_end`), and a footer row
/// count that is not the sum of the stripes' row counts.
fn validate_stripe_layout(footer: &Footer, stripes_end: u64) -> Result<()> {
    let mut next_offset = ORC_MAGIC_LENGTH;
    let mut total_rows = 0u64;
    for (index, stripe) in footer.stripes.iter().enumerate() {
        let end = stripe
            .offset()
            .checked_add(stripe.index_length())
            .and_then(|end| end.checked_add(stripe.data_length()))
            .and_then(|end| end.checked_add(stripe.footer_length()))
            .context(OutOfSpecSnafu {
                msg: format!("Stripe {index} extent overflows"),
            })?;
        ensure!(
            stripe.offset() >= next_offset,
            OutOfSpecSnafu {
                msg: format!(
                    "Stripe {index} starts at {} before the end of the preceding data at {next_offset}",
                    stripe.offset()
                )
            }
        );
        ensure!(
            end <= stripes_end,
            OutOfSpecSnafu {
                msg: format!(
                    "Stripe {index} ends at {end} after the start of the file tail at {stripes_end}"
                )
            }
        );
        next_offset = end;
        total_rows = total_rows
            .checked_add(stripe.number_of_rows())
            .context(OutOfSpecSnafu {
                msg: "Stripe row counts overflow",
            })?;
    }
    ensure!(
        footer.encryption.is_some() || next_offset == stripes_end,
        OutOfSpecSnafu {
            msg: format!(
                "The last stripe ends at {next_offset} but the file tail starts at {stripes_end}"
            )
        }
    );
    if let Some(number_of_rows) = footer.number_of_rows {
        ensure!(
            number_of_rows == total_rows,
            OutOfSpecSnafu {
                msg: format!(
                    "Footer states {number_of_rows} rows but its stripes hold {total_rows}"
                )
            }
        );
    }
    Ok(())
}

/// Refuses column statistics that are not one per type, or that carry the typed statistics
/// of a kind other than their column's.
fn validate_statistics(
    types: &[proto::Type],
    statistics: &[proto::ColumnStatistics],
) -> Result<()> {
    if statistics.is_empty() {
        return Ok(());
    }
    ensure!(
        statistics.len() == types.len(),
        OutOfSpecSnafu {
            msg: format!(
                "{} column statistics for {} columns",
                statistics.len(),
                types.len()
            )
        }
    );
    for (column, (ty, statistics)) in types.iter().zip(statistics).enumerate() {
        use proto::r#type::Kind;

        let present = [
            statistics.int_statistics.is_some(),
            statistics.double_statistics.is_some(),
            statistics.string_statistics.is_some(),
            statistics.bucket_statistics.is_some(),
            statistics.decimal_statistics.is_some(),
            statistics.date_statistics.is_some(),
            statistics.binary_statistics.is_some(),
            statistics.timestamp_statistics.is_some(),
        ];
        let allowed = match ty.kind() {
            Kind::Byte | Kind::Short | Kind::Int | Kind::Long => 0,
            Kind::Float | Kind::Double => 1,
            Kind::String | Kind::Varchar | Kind::Char => 2,
            Kind::Boolean => 3,
            Kind::Decimal => 4,
            Kind::Date => 5,
            Kind::Binary => 6,
            Kind::Timestamp | Kind::TimestampInstant => 7,
            Kind::Struct | Kind::List | Kind::Map | Kind::Union => present.len(),
        };
        let range = match ty.kind() {
            Kind::Byte => i64::from(i8::MIN)..=i64::from(i8::MAX),
            Kind::Short => i64::from(i16::MIN)..=i64::from(i16::MAX),
            Kind::Int => i64::from(i32::MIN)..=i64::from(i32::MAX),
            _ => i64::MIN..=i64::MAX,
        };
        ensure!(
            statistics.int_statistics.as_ref().map_or(true, |int| {
                int.minimum
                    .iter()
                    .chain(&int.maximum)
                    .all(|v| range.contains(v))
            }),
            OutOfSpecSnafu {
                msg: format!(
                    "column {column} of type {:?} has statistics outside the type's range",
                    ty.kind()
                )
            }
        );
        ensure!(
            present
                .iter()
                .enumerate()
                .all(|(index, present)| !present || index == allowed),
            OutOfSpecSnafu {
                msg: format!(
                    "column {column} of type {:?} has statistics of another type",
                    ty.kind()
                )
            }
        );
    }
    Ok(())
}

fn deserialize_footer(
    bytes: Bytes,
    compression: Option<Compression>,
    limit: usize,
) -> Result<(Footer, usize)> {
    let buffer = read_decompressed_section_with_limit(bytes, compression, limit)?;
    let size = buffer.len();
    let footer = Footer::decode(buffer.as_slice()).context(error::DecodeProtoSnafu)?;
    FOOTER.ensure_singular_fields_once(&buffer)?;
    Ok((footer, size))
}

fn deserialize_footer_metadata(
    bytes: Bytes,
    compression: Option<Compression>,
    limit: usize,
) -> Result<(Metadata, usize)> {
    let buffer = read_decompressed_section_with_limit(bytes, compression, limit)?;
    let size = buffer.len();
    let metadata = Metadata::decode(buffer.as_slice()).context(error::DecodeProtoSnafu)?;
    Ok((metadata, size))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::{ArrowReaderBuilder, ArrowWriterBuilder};

    fn two_rows() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7, 11]))]).unwrap()
    }

    fn without_metadata_length(batch: &RecordBatch) -> Bytes {
        let mut file = Vec::new();
        let mut writer = ArrowWriterBuilder::new(&mut file, batch.schema())
            .try_build()
            .unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();

        let postscript_start = file.len() - 1 - *file.last().unwrap() as usize;
        let mut postscript = PostScript::decode(&file[postscript_start..file.len() - 1]).unwrap();
        assert_eq!(postscript.metadata_length, Some(0));
        postscript.metadata_length = None;
        let postscript = postscript.encode_to_vec();
        file.truncate(postscript_start);
        file.extend_from_slice(&postscript);
        file.push(postscript.len() as u8);
        Bytes::from(file)
    }

    #[test]
    fn an_absent_metadata_length_reads_as_an_empty_metadata_section() {
        let batch = two_rows();
        let rows = ArrowReaderBuilder::try_new(without_metadata_length(&batch))
            .unwrap()
            .build()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows, [batch]);
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn an_absent_metadata_length_reads_as_an_empty_metadata_section_async() {
        use futures_util::TryStreamExt;

        let batch = two_rows();
        let file = std::io::Cursor::new(without_metadata_length(&batch));
        let rows = ArrowReaderBuilder::try_new_async(file)
            .await
            .unwrap()
            .build_async()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(rows, [batch]);
    }
}
