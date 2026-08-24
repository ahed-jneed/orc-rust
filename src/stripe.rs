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

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use prost::Message;
use snafu::{ensure, OptionExt, ResultExt};

use crate::{
    column::Column,
    compression::{read_decompressed_section, Compression, Decompressor},
    error::{self, IoSnafu, Result},
    proto::{self, stream::Kind, StripeFooter},
    reader::{metadata::FileMetadata, ChunkReader},
    schema::RootDataType,
    statistics::ColumnStatistics,
};

/// Stripe metadata parsed from the file tail metadata sections.
/// Does not contain the actual stripe bytes, as those are decoded
/// when they are required.
#[derive(Debug, Clone)]
pub struct StripeMetadata {
    /// Statistics of columns across this specific stripe
    column_statistics: Vec<ColumnStatistics>,
    /// Byte offset of start of stripe from start of file
    offset: u64,
    /// Byte length of index section
    index_length: u64,
    /// Byte length of data section
    data_length: u64,
    /// Byte length of footer section
    footer_length: u64,
    /// Number of rows in the stripe
    number_of_rows: u64,
}

/// A stripe retains selected streams in memory while its arrays are decoded.
/// Bound the aggregate rather than only each individual stream request.
const MAX_STRIPE_STREAM_BYTES: u64 = 512 * 1024 * 1024;

impl StripeMetadata {
    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn index_length(&self) -> u64 {
        self.index_length
    }

    pub fn data_length(&self) -> u64 {
        self.data_length
    }

    pub fn footer_length(&self) -> u64 {
        self.footer_length
    }

    pub fn number_of_rows(&self) -> u64 {
        self.number_of_rows
    }

    pub fn column_statistics(&self) -> &[ColumnStatistics] {
        &self.column_statistics
    }

    pub fn footer_offset(&self) -> Result<u64> {
        self.offset
            .checked_add(self.index_length)
            .and_then(|offset| offset.checked_add(self.data_length))
            .context(error::OutOfSpecSnafu {
                msg: "stripe footer offset overflows",
            })
    }
}

impl TryFrom<(&proto::StripeInformation, &proto::StripeStatistics)> for StripeMetadata {
    type Error = error::OrcError;

    fn try_from(value: (&proto::StripeInformation, &proto::StripeStatistics)) -> Result<Self> {
        let (info, statistics) = value;
        let column_statistics = statistics
            .col_stats
            .iter()
            .map(TryFrom::try_from)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            column_statistics,
            offset: info.offset(),
            index_length: info.index_length(),
            data_length: info.data_length(),
            footer_length: info.footer_length(),
            number_of_rows: info.number_of_rows(),
        })
    }
}

impl TryFrom<&proto::StripeInformation> for StripeMetadata {
    type Error = error::OrcError;

    fn try_from(value: &proto::StripeInformation) -> Result<Self> {
        Ok(Self {
            column_statistics: vec![],
            offset: value.offset(),
            index_length: value.index_length(),
            data_length: value.data_length(),
            footer_length: value.footer_length(),
            number_of_rows: value.number_of_rows(),
        })
    }
}

#[derive(Debug)]
pub struct Stripe {
    columns: Vec<Column>,
    stream_map: StreamMap,
    number_of_rows: usize,
    tz: Option<chrono_tz::Tz>,
}

impl Stripe {
    pub fn new<R: ChunkReader>(
        reader: &mut R,
        file_metadata: &FileMetadata,
        projected_data_type: &RootDataType,
        info: &StripeMetadata,
    ) -> Result<Self> {
        let footer = reader
            .get_bytes(info.footer_offset()?, info.footer_length())
            .context(IoSnafu)?;
        let footer = Arc::new(deserialize_stripe_footer(
            footer,
            file_metadata.compression(),
        )?);

        let columns: Vec<Column> = projected_data_type
            .children()
            .iter()
            .map(|col| {
                Column::new(
                    col.name().to_string(),
                    col.data_type().clone(),
                    footer.clone(),
                )
            })
            .collect();

        validate_stream_layout(projected_data_type, &footer, info.offset())?;
        let mut stream_map = HashMap::new();
        let mut stream_offset = info.offset();
        for stream in &footer.streams {
            let length = stream.length();
            let column_id = stream.column();
            if projected_data_type.contains_column_index(column_id as usize) {
                let kind = stream.kind();
                let data = reader.get_bytes(stream_offset, length).context(IoSnafu)?;
                stream_map.insert((column_id, kind), data);
            }
            stream_offset = stream_offset
                .checked_add(length)
                .context(error::OutOfSpecSnafu {
                    msg: "stripe stream offsets overflow",
                })?;
        }

        let tz = footer
            .writer_timezone
            .as_ref()
            .map(|a| {
                a.parse::<chrono_tz::Tz>()
                    .map_err(|_| crate::error::OrcError::OutOfSpec {
                        msg: format!("invalid writer timezone '{a}'"),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    })
            })
            .transpose()?;

        Ok(Self {
            columns,
            stream_map: StreamMap {
                inner: stream_map,
                compression: file_metadata.compression(),
            },
            number_of_rows: usize::try_from(info.number_of_rows()).map_err(|_| {
                error::OutOfSpecSnafu {
                    msg: "stripe row count does not fit in usize",
                }
                .build()
            })?,
            tz,
        })
    }

    // TODO: reduce duplication with above
    #[cfg(feature = "async")]
    pub async fn new_async<R: crate::reader::AsyncChunkReader>(
        reader: &mut R,
        file_metadata: &FileMetadata,
        projected_data_type: &RootDataType,
        info: &StripeMetadata,
    ) -> Result<Self> {
        let footer = reader
            .get_bytes(info.footer_offset()?, info.footer_length())
            .await
            .context(IoSnafu)?;
        let footer = Arc::new(deserialize_stripe_footer(
            footer,
            file_metadata.compression(),
        )?);

        let columns: Vec<Column> = projected_data_type
            .children()
            .iter()
            .map(|col| {
                Column::new(
                    col.name().to_string(),
                    col.data_type().clone(),
                    footer.clone(),
                )
            })
            .collect();

        validate_stream_layout(projected_data_type, &footer, info.offset())?;
        let mut stream_map = HashMap::new();
        let mut stream_offset = info.offset();
        for stream in &footer.streams {
            let length = stream.length();
            let column_id = stream.column();
            if projected_data_type.contains_column_index(column_id as usize) {
                let kind = stream.kind();
                let data = reader
                    .get_bytes(stream_offset, length)
                    .await
                    .context(IoSnafu)?;
                stream_map.insert((column_id, kind), data);
            }

            stream_offset = stream_offset
                .checked_add(length)
                .context(error::OutOfSpecSnafu {
                    msg: "stripe stream offsets overflow",
                })?;
        }

        let tz = footer
            .writer_timezone
            .as_ref()
            .map(|a| {
                a.parse::<chrono_tz::Tz>()
                    .map_err(|_| crate::error::OrcError::OutOfSpec {
                        msg: format!("invalid writer timezone '{a}'"),
                        location: snafu::Location::new(file!(), line!(), column!()),
                    })
            })
            .transpose()?;

        Ok(Self {
            columns,
            stream_map: StreamMap {
                inner: stream_map,
                compression: file_metadata.compression(),
            },
            number_of_rows: usize::try_from(info.number_of_rows()).map_err(|_| {
                error::OutOfSpecSnafu {
                    msg: "stripe row count does not fit in usize",
                }
                .build()
            })?,
            tz,
        })
    }

    pub fn number_of_rows(&self) -> usize {
        self.number_of_rows
    }

    pub fn stream_map(&self) -> &StreamMap {
        &self.stream_map
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn writer_tz(&self) -> Option<chrono_tz::Tz> {
        self.tz
    }

    /// Parse row indexes from the stripe index section
    ///
    /// According to ORC spec, row indexes are only loaded when predicate pushdown
    /// is used or when seeking to a particular row. This function performs lazy
    /// parsing of ROW_INDEX streams from the stripe's index section.
    ///
    /// # Arguments
    ///
    /// * `file_metadata` - File metadata containing row_index_stride
    ///
    /// # Returns
    ///
    /// * `Ok(StripeRowIndex)` - Parsed row indexes for all primitive columns
    /// * `Err(OrcError)` - If parsing fails or row_index_stride is not available
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use orc_rust::stripe::Stripe;
    /// # use orc_rust::reader::metadata::FileMetadata;
    /// # fn example(stripe: &Stripe, file_metadata: &FileMetadata) -> Result<(), Box<dyn std::error::Error>> {
    /// let row_index = stripe.read_row_indexes(file_metadata)?;
    /// // Access statistics for each row group
    /// if let Some(col_index) = row_index.column(0) {
    ///     for row_group_idx in 0..col_index.num_row_groups() {
    ///         if let Some(stats) = col_index.row_group_stats(row_group_idx) {
    ///             println!("Row group {}: {:?}", row_group_idx, stats);
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn read_row_indexes(
        &self,
        file_metadata: &crate::reader::metadata::FileMetadata,
    ) -> Result<crate::row_index::StripeRowIndex> {
        let rows_per_group = file_metadata.row_index_stride().unwrap_or(10_000); // Default per ORC spec

        crate::row_index::parse_stripe_row_indexes(
            &self.stream_map,
            &self.columns,
            self.number_of_rows,
            rows_per_group,
        )
    }
}

#[derive(Debug)]
pub struct StreamMap {
    /// <(ColumnId, Kind), Bytes>
    inner: HashMap<(u32, Kind), Bytes>,
    compression: Option<Compression>,
}

impl StreamMap {
    pub fn get(&self, column: &Column, kind: Kind) -> Decompressor {
        // There is edge case where if column has no data then the stream might be omitted entirely
        // (e.g. if there is only 1 null element, then it'll have present stream, but no data stream)
        // See the integration::meta_data test for an example of this
        // TODO: some better way to handle this?
        self.get_opt(column, kind)
            .unwrap_or_else(Decompressor::empty)
    }

    pub fn get_opt(&self, column: &Column, kind: Kind) -> Option<Decompressor> {
        let column_id = column.column_id();

        self.inner
            .get(&(column_id, kind))
            .cloned()
            .map(|data| Decompressor::new(data, self.compression, vec![]))
    }
}

fn deserialize_stripe_footer(
    bytes: Bytes,
    compression: Option<Compression>,
) -> Result<StripeFooter> {
    let buffer = read_decompressed_section(bytes, compression)?;
    StripeFooter::decode(buffer.as_slice()).context(error::DecodeProtoSnafu)
}

fn validate_stream_layout(
    projected_data_type: &RootDataType,
    footer: &StripeFooter,
    start_offset: u64,
) -> Result<()> {
    let mut stream_offset = start_offset;
    let mut selected_bytes = 0u64;
    for stream in &footer.streams {
        let length = stream.length();
        if projected_data_type.contains_column_index(stream.column() as usize) {
            selected_bytes = selected_bytes
                .checked_add(length)
                .context(error::OutOfSpecSnafu {
                    msg: "selected stripe stream bytes overflow",
                })?;
            ensure!(
                selected_bytes <= MAX_STRIPE_STREAM_BYTES,
                error::OutOfSpecSnafu {
                    msg: format!(
                        "selected stripe streams exceed {MAX_STRIPE_STREAM_BYTES}-byte limit"
                    )
                }
            );
        }
        stream_offset = stream_offset
            .checked_add(length)
            .context(error::OutOfSpecSnafu {
                msg: "stripe stream offsets overflow",
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_offset_overflow_is_rejected() {
        let metadata = StripeMetadata {
            column_statistics: vec![],
            offset: u64::MAX,
            index_length: 1,
            data_length: 0,
            footer_length: 0,
            number_of_rows: 0,
        };
        assert!(metadata.footer_offset().is_err());
    }

    #[test]
    fn selected_stream_budget_is_cumulative() {
        use crate::proto::{r#type::Kind as TypeKind, stream::Kind, Stream, StripeFooter, Type};

        let schema = RootDataType::from_proto(&[
            Type {
                kind: Some(TypeKind::Struct.into()),
                subtypes: vec![1],
                field_names: vec!["value".to_string()],
                ..Default::default()
            },
            Type {
                kind: Some(TypeKind::Int.into()),
                ..Default::default()
            },
        ])
        .unwrap();
        let stream = |length| Stream {
            kind: Some(Kind::Data.into()),
            column: Some(1),
            length: Some(length),
        };
        let footer = StripeFooter {
            streams: vec![stream(300 * 1024 * 1024), stream(300 * 1024 * 1024)],
            ..Default::default()
        };
        assert!(validate_stream_layout(&schema, &footer, 0).is_err());
    }
}
