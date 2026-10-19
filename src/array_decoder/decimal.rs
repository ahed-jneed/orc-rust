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

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::buffer::NullBuffer;
use arrow::datatypes::{Decimal128Type, DecimalType};
use snafu::{ensure, OptionExt, ResultExt};

use crate::encoding::decimal::UnboundedVarintStreamDecoder;
use crate::encoding::integer::{get_signed_int_decoder, RleVersion};
use crate::encoding::PrimitiveValueDecoder;
use crate::error::{ArrowSnafu, DecimalScaleLossSnafu, InvalidDecimalTypeSnafu, OutOfSpecSnafu};
use crate::proto::column_encoding::Kind as EncodingKind;
use crate::proto::stream::Kind;
use crate::stripe::Stripe;
use crate::{column::Column, error::Result};

use super::{ArrayBatchDecoder, PresentDecoder, PrimitiveArrayDecoder};

const MAX_DECIMAL64_PRECISION: u8 = 18;

struct Decimal64Decoder {
    iter: Box<dyn PrimitiveValueDecoder<i64> + Send>,
}

impl PrimitiveValueDecoder<i128> for Decimal64Decoder {
    fn skip(&mut self, n: usize) -> Result<()> {
        self.iter.skip(n)
    }

    fn decode(&mut self, out: &mut [i128]) -> Result<()> {
        let mut values = vec![0; out.len()];
        self.iter.decode(&mut values)?;
        for (o, v) in out.iter_mut().zip(values) {
            *o = i128::from(v);
        }
        Ok(())
    }
}

enum DecimalLayout {
    Decimal64,
    Legacy,
}

pub fn new_decimal_decoder(
    column: &Column,
    stripe: &Stripe,
    precision: u32,
    fixed_scale: u32,
    scale_unspecified: bool,
) -> Result<Box<dyn ArrayBatchDecoder>> {
    let (precision, scale) = decimal_type(column, precision, fixed_scale)?;
    if let DecimalLayout::Decimal64 = decimal_layout(column, stripe, precision)? {
        let data = stripe.stream_map().get(column, Kind::Data);
        let iter = get_signed_int_decoder::<i64>(data, RleVersion::V2);
        let present = PresentDecoder::from_stripe(stripe, column);
        return Ok(Box::new(DecimalArrayDecoder::new(
            precision,
            scale,
            Box::new(Decimal64Decoder { iter }),
            present,
        )));
    }

    let varint_iter = stripe.stream_map().get(column, Kind::Data);
    let varint_iter = Box::new(UnboundedVarintStreamDecoder::new(varint_iter));

    // Scale is specified on a per varint basis (in addition to being encoded in the type)
    let scale_iter = stripe.stream_map().get(column, Kind::Secondary);
    let scale_iter = get_signed_int_decoder::<i32>(scale_iter, column.rle_version());

    let present = PresentDecoder::from_stripe(stripe, column);

    let iter = DecimalScaleRepairDecoder {
        varint_iter,
        scale_iter,
        fixed_scale,
        unspecified_scale: scale_unspecified.then(|| UnspecifiedScale {
            column: column.name().to_string(),
            precision,
        }),
    };
    let iter = Box::new(iter);

    Ok(Box::new(DecimalArrayDecoder::new(
        precision, scale, iter, present,
    )))
}

fn decimal_type(column: &Column, precision: u32, scale: u32) -> Result<(u8, i8)> {
    let invalid = || {
        InvalidDecimalTypeSnafu {
            column: column.name().to_string(),
            precision,
            scale,
        }
        .build()
    };
    let narrow_precision = u8::try_from(precision)
        .ok()
        .filter(|precision| (1..=Decimal128Type::MAX_PRECISION).contains(precision))
        .ok_or_else(invalid)?;
    let narrow_scale = u8::try_from(scale)
        .ok()
        .filter(|scale| *scale <= narrow_precision)
        .and_then(|scale| i8::try_from(scale).ok())
        .ok_or_else(invalid)?;
    Ok((narrow_precision, narrow_scale))
}

fn decimal_layout(column: &Column, stripe: &Stripe, precision: u8) -> Result<DecimalLayout> {
    let mut present = 0;
    let mut data = 0;
    let mut secondary = 0;
    let mut row_index = 0;
    let mut bloom_filter = 0;
    let mut bloom_filter_utf8 = 0;
    for kind in column.stream_kinds() {
        let count = match kind {
            Some(Kind::Present) => &mut present,
            Some(Kind::Data) => &mut data,
            Some(Kind::Secondary) => &mut secondary,
            Some(Kind::RowIndex) => &mut row_index,
            Some(Kind::BloomFilter) => &mut bloom_filter,
            Some(Kind::BloomFilterUtf8) => &mut bloom_filter_utf8,
            other => {
                return OutOfSpecSnafu {
                    msg: format!(
                        "decimal column '{}' has an unexpected {other:?} stream",
                        column.name()
                    ),
                }
                .fail()
            }
        };
        *count += 1;
        ensure!(
            *count == 1,
            OutOfSpecSnafu {
                msg: format!(
                    "decimal column '{}' has more than one {kind:?} stream",
                    column.name()
                ),
            }
        );
    }
    if precision <= MAX_DECIMAL64_PRECISION && stripe.decimal64_file() {
        ensure!(
            secondary == 0,
            OutOfSpecSnafu {
                msg: format!(
                    "decimal column '{}' of precision {precision} has a SECONDARY stream in a Decimal64 file",
                    column.name()
                ),
            }
        );
        ensure!(
            matches!(
                column.encoding().kind(),
                EncodingKind::Direct | EncodingKind::DirectV2
            ),
            OutOfSpecSnafu {
                msg: format!(
                    "decimal column '{}' of a Decimal64 file has encoding {:?}",
                    column.name(),
                    column.encoding().kind()
                ),
            }
        );
        return Ok(DecimalLayout::Decimal64);
    }
    ensure!(
        data == secondary,
        OutOfSpecSnafu {
            msg: format!(
                "decimal column '{}' needs both a DATA and a SECONDARY stream or neither",
                column.name()
            ),
        }
    );
    Ok(DecimalLayout::Legacy)
}

/// Wrapper around PrimitiveArrayDecoder to allow specifying the precision and scale
/// of the output decimal array.
pub struct DecimalArrayDecoder {
    precision: u8,
    scale: i8,
    inner: PrimitiveArrayDecoder<Decimal128Type>,
}

impl DecimalArrayDecoder {
    pub fn new(
        precision: u8,
        scale: i8,
        iter: Box<dyn PrimitiveValueDecoder<i128> + Send>,
        present: Option<PresentDecoder>,
    ) -> Self {
        let inner = PrimitiveArrayDecoder::<Decimal128Type>::new(iter, present);
        Self {
            precision,
            scale,
            inner,
        }
    }
}

impl ArrayBatchDecoder for DecimalArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let array = self
            .inner
            .next_primitive_batch(batch_size, parent_present)?
            .with_precision_and_scale(self.precision, self.scale)
            .context(ArrowSnafu)?;
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        self.inner.skip_values(n, parent_present)
    }
}

/// This iter fixes the scales of the varints decoded as scale is specified on a per
/// varint basis, and needs to align with type specified scale
struct DecimalScaleRepairDecoder {
    varint_iter: Box<dyn PrimitiveValueDecoder<i128> + Send>,
    scale_iter: Box<dyn PrimitiveValueDecoder<i32> + Send>,
    fixed_scale: u32,
    unspecified_scale: Option<UnspecifiedScale>,
}

struct UnspecifiedScale {
    column: String,
    precision: u8,
}

impl PrimitiveValueDecoder<i128> for DecimalScaleRepairDecoder {
    fn skip(&mut self, n: usize) -> Result<()> {
        self.varint_iter.skip(n)?;
        self.scale_iter.skip(n)?;
        Ok(())
    }

    fn decode(&mut self, out: &mut [i128]) -> Result<()> {
        // TODO: can probably optimize, reuse buffers?
        let mut varint = vec![0; out.len()];
        let mut scale = vec![0; out.len()];
        self.varint_iter.decode(&mut varint)?;
        self.scale_iter.decode(&mut scale)?;
        for (index, (&varint, &scale)) in varint.iter().zip(scale.iter()).enumerate() {
            let column = self.unspecified_scale.as_ref().map(|u| u.column.as_str());
            out[index] = fix_i128_scale(varint, self.fixed_scale, scale, column)?;
            if let Some(unspecified) = &self.unspecified_scale {
                ensure!(
                    Decimal128Type::is_valid_decimal_precision(out[index], unspecified.precision),
                    OutOfSpecSnafu {
                        msg: format!(
                            "decimal value in column '{}' does not fit precision {} at scale {}",
                            unspecified.column, unspecified.precision, self.fixed_scale
                        ),
                    }
                );
            }
        }
        Ok(())
    }
}

fn fix_i128_scale(
    i: i128,
    fixed_scale: u32,
    varying_scale: i32,
    lossless_column: Option<&str>,
) -> Result<i128> {
    let varying_scale = u32::try_from(varying_scale).ok().context(OutOfSpecSnafu {
        msg: "decimal value has a negative scale",
    })?;
    match fixed_scale.cmp(&varying_scale) {
        Ordering::Less => {
            // fixed_scale < varying_scale
            // Current scale of number is greater than scale of the array type
            // So need to divide to align the scale
            // TODO: this differs from C++ implementation, need to verify
            let scale_factor = varying_scale - fixed_scale;
            if let Some(column) = lossless_column {
                let lossy = || {
                    DecimalScaleLossSnafu {
                        column,
                        value_scale: varying_scale,
                        column_scale: fixed_scale,
                    }
                    .build()
                };
                return match 10_i128.checked_pow(scale_factor) {
                    Some(scale_factor) if i % scale_factor == 0 => Ok(i / scale_factor),
                    Some(_) => Err(lossy()),
                    None if i == 0 => Ok(0),
                    None => Err(lossy()),
                };
            }
            // TODO: replace with lookup table?
            let scale_factor = scale_factor_pow10(scale_factor)?;
            Ok(i / scale_factor)
        }
        Ordering::Equal => Ok(i),
        Ordering::Greater => {
            // fixed_scale > varying_scale
            // Current scale of number is smaller than scale of the array type
            // So need to multiply to align the scale
            // TODO: this differs from C++ implementation, need to verify
            let scale_factor = fixed_scale - varying_scale;
            // TODO: replace with lookup table?
            let scale_factor = scale_factor_pow10(scale_factor)?;
            i.checked_mul(scale_factor).context(OutOfSpecSnafu {
                msg: "decimal value overflows when aligning scale",
            })
        }
    }
}

fn scale_factor_pow10(exponent: u32) -> Result<i128> {
    10_i128.checked_pow(exponent).context(OutOfSpecSnafu {
        msg: "decimal scale difference is out of range",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::StripeFooter;
    use crate::schema::DataType;

    fn decimal_column() -> Column {
        let data_type = DataType::Decimal {
            column_index: 0,
            precision: 10,
            scale: 2,
            scale_unspecified: false,
        };
        Column::new(
            "d".to_string(),
            data_type,
            Arc::new(StripeFooter::default()),
        )
    }

    #[test]
    fn decimal_type_bounds() {
        let column = decimal_column();
        assert_eq!(decimal_type(&column, 1, 0).unwrap(), (1, 0));
        assert_eq!(decimal_type(&column, 1, 1).unwrap(), (1, 1));
        assert_eq!(decimal_type(&column, 38, 38).unwrap(), (38, 38));
        assert_eq!(decimal_type(&column, 12, 2).unwrap(), (12, 2));
        for (precision, scale) in [
            (0, 0),
            (1, 2),
            (12, 13),
            (12, 127),
            (12, 128),
            (12, 255),
            (12, 256),
            (38, 39),
            (39, 2),
            (256, 0),
            (u32::MAX, 0),
            (12, u32::MAX),
        ] {
            assert!(
                decimal_type(&column, precision, scale).is_err(),
                "({precision}, {scale})"
            );
        }
    }

    #[test]
    fn negative_value_scale_is_rejected() {
        assert!(fix_i128_scale(0, 2, -1, Some("a")).is_err());
        assert!(fix_i128_scale(7, 2, -1, None).is_err());
        assert_eq!(fix_i128_scale(7, 2, 2, None).unwrap(), 7);
    }
}
