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

//! Checks of the protobuf wire format of the file metadata that the generated decoders do
//! not make.
//!
//! The decoders keep the last of a non-repeated field that occurs several times, so a
//! damaged tag that turns one field into another silently overrides the value that was
//! written. No ORC writer emits a non-repeated field twice.

use snafu::ensure;

use crate::error::{OutOfSpecSnafu, Result};

/// The fields of a message that may occur at most once, and the messages nested in its
/// other fields.
pub(crate) struct Message {
    name: &'static str,
    singular: &'static [u64],
    nested: &'static [(u64, &'static Message)],
}

pub(crate) static POSTSCRIPT: Message = Message {
    name: "the postscript",
    singular: &[1, 2, 3, 5, 6, 8000],
    nested: &[],
};

static STRIPE_INFORMATION: Message = Message {
    name: "a stripe information",
    singular: &[1, 2, 3, 4, 5, 6],
    nested: &[],
};

static TYPE: Message = Message {
    name: "a type",
    singular: &[1, 4, 5, 6],
    nested: &[],
};

pub(crate) static FOOTER: Message = Message {
    name: "the footer",
    singular: &[1, 2, 6, 8, 9, 10, 11, 12],
    nested: &[(3, &STRIPE_INFORMATION), (4, &TYPE)],
};

static STREAM: Message = Message {
    name: "a stream",
    singular: &[1, 2, 3],
    nested: &[],
};

static COLUMN_ENCODING: Message = Message {
    name: "a column encoding",
    singular: &[1, 2, 3],
    nested: &[],
};

pub(crate) static STRIPE_FOOTER: Message = Message {
    name: "a stripe footer",
    singular: &[3],
    nested: &[(1, &STREAM), (2, &COLUMN_ENCODING)],
};

fn read_varint(buf: &[u8], at: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = *buf.get(*at)?;
        *at += 1;
        value |= u64::from(byte & 0x7F).checked_shl(shift)?;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

impl Message {
    /// Refuses `buf`, an encoded message that already decoded, if a field that may occur
    /// once occurs more often in it or in a message nested in it.
    pub(crate) fn ensure_singular_fields_once(&self, buf: &[u8]) -> Result<()> {
        let mut seen = Vec::with_capacity(self.singular.len());
        let mut at = 0;
        while at < buf.len() {
            let Some(tag) = read_varint(buf, &mut at) else {
                return Ok(());
            };
            let field = tag >> 3;
            let skipped = match tag & 7 {
                0 => read_varint(buf, &mut at).map(|_| 0),
                1 => Some(8),
                2 => read_varint(buf, &mut at).map(|length| length as usize),
                5 => Some(4),
                _ => return Ok(()),
            };
            let Some(length) = skipped else {
                return Ok(());
            };
            let Some(end) = at.checked_add(length).filter(|end| *end <= buf.len()) else {
                return Ok(());
            };
            if self.singular.contains(&field) {
                ensure!(
                    !seen.contains(&field),
                    OutOfSpecSnafu {
                        msg: format!("field {field} of {} occurs more than once", self.name)
                    }
                );
                seen.push(field);
            }
            if tag & 7 == 2 {
                if let Some((_, nested)) = self.nested.iter().find(|(number, _)| *number == field) {
                    nested.ensure_singular_fields_once(&buf[at..end])?;
                }
            }
            at = end;
        }
        Ok(())
    }
}
