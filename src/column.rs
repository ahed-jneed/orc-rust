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

use std::sync::Arc;

use crate::encoding::integer::RleVersion;
use crate::proto::{
    column_encoding::Kind as ProtoColumnKind, stream::Kind as ProtoStreamKind, ColumnEncoding,
    StripeFooter,
};
use crate::schema::DataType;

#[derive(Clone, Debug)]
pub struct Column {
    footer: Arc<StripeFooter>,
    name: String,
    data_type: DataType,
}

impl Column {
    pub fn new(name: String, data_type: DataType, footer: Arc<StripeFooter>) -> Self {
        Self {
            footer,
            data_type,
            name,
        }
    }

    pub fn dictionary_size(&self) -> usize {
        let column = self.data_type.column_index();
        self.footer.columns[column]
            .dictionary_size
            .unwrap_or_default() as usize
    }

    pub fn encoding(&self) -> ColumnEncoding {
        let column = self.data_type.column_index();
        self.footer.columns[column].clone()
    }

    /// The kinds of all streams the stripe footer lists for this column, in footer order.
    /// A kind this reader does not know is reported as `None`.
    pub fn stream_kinds(&self) -> Vec<Option<ProtoStreamKind>> {
        let column = self.column_id();
        self.footer
            .streams
            .iter()
            .filter(|stream| stream.column() == column)
            .map(|stream| ProtoStreamKind::try_from(stream.kind.unwrap_or_default()).ok())
            .collect()
    }

    pub fn rle_version(&self) -> RleVersion {
        // TODO: Validity check for this? e.g. ensure INT column isn't dictionary encoded.
        //       Or maybe check that at init time; to ensure we catch at earliest opportunity?
        match self.encoding().kind() {
            ProtoColumnKind::Direct | ProtoColumnKind::Dictionary => RleVersion::V1,
            ProtoColumnKind::DirectV2 | ProtoColumnKind::DictionaryV2 => RleVersion::V2,
        }
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn column_id(&self) -> u32 {
        self.data_type.column_index() as u32
    }

    pub fn children(&self) -> Vec<Column> {
        match &self.data_type {
            DataType::Boolean { .. }
            | DataType::Byte { .. }
            | DataType::Short { .. }
            | DataType::Int { .. }
            | DataType::Long { .. }
            | DataType::Float { .. }
            | DataType::Double { .. }
            | DataType::String { .. }
            | DataType::Varchar { .. }
            | DataType::Char { .. }
            | DataType::Binary { .. }
            | DataType::Decimal { .. }
            | DataType::Timestamp { .. }
            | DataType::TimestampWithLocalTimezone { .. }
            | DataType::Date { .. } => vec![],
            DataType::Struct { children, .. } => children
                .iter()
                .map(|col| Column {
                    footer: self.footer.clone(),
                    name: col.name().to_string(),
                    data_type: col.data_type().clone(),
                })
                .collect(),
            DataType::List { child, .. } => {
                vec![Column {
                    footer: self.footer.clone(),
                    name: "item".to_string(),
                    data_type: *child.clone(),
                }]
            }
            DataType::Map { key, value, .. } => {
                vec![
                    Column {
                        footer: self.footer.clone(),
                        name: "key".to_string(),
                        data_type: *key.clone(),
                    },
                    Column {
                        footer: self.footer.clone(),
                        name: "value".to_string(),
                        data_type: *value.clone(),
                    },
                ]
            }
            DataType::Union { variants, .. } => {
                // TODO: might need corrections
                variants
                    .iter()
                    .enumerate()
                    .map(|(index, data_type)| Column {
                        footer: self.footer.clone(),
                        name: format!("{index}"),
                        data_type: data_type.clone(),
                    })
                    .collect()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::Stream;

    #[test]
    fn stream_kinds_of_the_column_only() {
        let stream = |column: u32, kind: Option<i32>| Stream {
            kind,
            column: Some(column),
            length: Some(0),
        };
        let footer = StripeFooter {
            streams: vec![
                stream(1, Some(ProtoStreamKind::Data as i32)),
                stream(2, Some(ProtoStreamKind::Secondary as i32)),
                stream(1, None),
                stream(1, Some(1000)),
                stream(1, Some(ProtoStreamKind::RowIndex as i32)),
            ],
            ..Default::default()
        };
        let data_type = DataType::Decimal {
            column_index: 1,
            precision: 10,
            scale: 2,
            scale_unspecified: false,
        };
        let column = Column::new("d".to_string(), data_type, Arc::new(footer));
        assert_eq!(
            column.stream_kinds(),
            vec![
                Some(ProtoStreamKind::Data),
                Some(ProtoStreamKind::Present),
                None,
                Some(ProtoStreamKind::RowIndex)
            ]
        );
    }
}
