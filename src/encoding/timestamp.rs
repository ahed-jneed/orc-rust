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

use std::marker::PhantomData;

use arrow::datatypes::{ArrowTimestampType, TimeUnit};

use crate::{
    encoding::PrimitiveValueDecoder,
    error::{DecodeTimestampSnafu, Result},
};

const NANOSECONDS_IN_SECOND: i64 = 1_000_000_000;

pub struct TimestampDecoder<T: ArrowTimestampType> {
    base_from_epoch: i64,
    data: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    secondary: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    _marker: PhantomData<T>,
}

impl<T: ArrowTimestampType> TimestampDecoder<T> {
    pub fn new(
        base_from_epoch: i64,
        data: Box<dyn PrimitiveValueDecoder<i64> + Send>,
        secondary: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    ) -> Self {
        Self {
            base_from_epoch,
            data,
            secondary,
            _marker: PhantomData,
        }
    }
}

impl<T: ArrowTimestampType> PrimitiveValueDecoder<T::Native> for TimestampDecoder<T> {
    fn skip(&mut self, n: usize) -> Result<()> {
        self.data.skip(n)?;
        self.secondary.skip(n)?;
        Ok(())
    }

    fn decode(&mut self, out: &mut [T::Native]) -> Result<()> {
        // TODO: can probably optimize, reuse buffers?
        let mut data = vec![0; out.len()];
        let mut secondary = vec![0; out.len()];
        self.data.decode(&mut data)?;
        self.secondary.decode(&mut secondary)?;
        for (index, (&seconds_since_orc_base, &nanoseconds)) in
            data.iter().zip(secondary.iter()).enumerate()
        {
            out[index] =
                decode_timestamp::<T>(self.base_from_epoch, seconds_since_orc_base, nanoseconds)?;
        }
        Ok(())
    }
}

/// Arrow TimestampNanosecond type cannot represent the full datetime range of
/// the ORC Timestamp type, so this iterator provides the ability to decode the
/// raw nanoseconds without restricting it to the Arrow TimestampNanosecond range.
pub struct TimestampNanosecondAsDecimalDecoder {
    base_from_epoch: i64,
    data: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    secondary: Box<dyn PrimitiveValueDecoder<i64> + Send>,
}

impl TimestampNanosecondAsDecimalDecoder {
    pub fn new(
        base_from_epoch: i64,
        data: Box<dyn PrimitiveValueDecoder<i64> + Send>,
        secondary: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    ) -> Self {
        Self {
            base_from_epoch,
            data,
            secondary,
        }
    }
}

impl PrimitiveValueDecoder<i128> for TimestampNanosecondAsDecimalDecoder {
    fn skip(&mut self, n: usize) -> Result<()> {
        self.data.skip(n)?;
        self.secondary.skip(n)?;
        Ok(())
    }

    fn decode(&mut self, out: &mut [i128]) -> Result<()> {
        // TODO: can probably optimize, reuse buffers?
        let mut data = vec![0; out.len()];
        let mut secondary = vec![0; out.len()];
        self.data.decode(&mut data)?;
        self.secondary.decode(&mut secondary)?;
        for (index, (&seconds_since_orc_base, &nanoseconds)) in
            data.iter().zip(secondary.iter()).enumerate()
        {
            out[index] =
                decode_timestamp_as_i128(self.base_from_epoch, seconds_since_orc_base, nanoseconds);
        }
        Ok(())
    }
}

fn decode(base: i64, seconds_since_orc_base: i64, nanoseconds: i64) -> (i128, i128, i128) {
    // Last 3 bits indicate how many trailing zeros were truncated
    let zeros = nanoseconds & 0x7;
    // The Apache ORC C++ writer stores the negative fraction pyarrow gives a
    // pre-1970 timestamp as is, so decode the value as signed, as its reader does
    let mut nanoseconds = i128::from(nanoseconds >> 3);
    // Multiply by powers of 10 to get back the trailing zeros
    if zeros != 0 {
        nanoseconds *= 10_i128.pow(zeros as u32 + 1);
    }
    let seconds_since_epoch = i128::from(seconds_since_orc_base) + i128::from(base);
    // Timestamps below the UNIX epoch with nanoseconds > 999_999 need to be
    // adjusted to have 1 second subtracted due to ORC-763:
    // https://issues.apache.org/jira/browse/ORC-763
    let seconds = if seconds_since_epoch < 0 && nanoseconds > 999_999 {
        seconds_since_epoch - 1
    } else {
        seconds_since_epoch
    };
    // Convert into nanoseconds since epoch, which Arrow uses as native representation
    // of timestamps
    // The timestamp may overflow i64 as ORC encodes them as a pair of (seconds, nanoseconds)
    // while we encode them as a single i64 of nanoseconds in Arrow.
    let nanoseconds_since_epoch = seconds * NANOSECONDS_IN_SECOND as i128 + nanoseconds;
    // Returning seconds & nanoseconds only for error message
    // TODO: does the error message really need those details? Can simplify by removing.
    (nanoseconds_since_epoch, seconds, nanoseconds)
}

fn decode_timestamp<T: ArrowTimestampType>(
    base: i64,
    seconds_since_orc_base: i64,
    nanoseconds: i64,
) -> Result<i64> {
    let (nanoseconds_since_epoch, seconds, nanoseconds) =
        decode(base, seconds_since_orc_base, nanoseconds);

    let nanoseconds_in_timeunit = match T::UNIT {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };

    // Truncate toward zero, as Arrow's cast between timestamp units does, then
    // convert to i64 and error if overflow
    let num_since_epoch = (nanoseconds_since_epoch / nanoseconds_in_timeunit)
        .try_into()
        .or_else(|_| {
            DecodeTimestampSnafu {
                seconds,
                nanoseconds,
                to_time_unit: T::UNIT,
            }
            .fail()
        })?;

    Ok(num_since_epoch)
}

fn decode_timestamp_as_i128(base: i64, seconds_since_orc_base: i64, nanoseconds: i64) -> i128 {
    let (nanoseconds_since_epoch, _, _) = decode(base, seconds_since_orc_base, nanoseconds);
    nanoseconds_since_epoch
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{TimestampMicrosecondType, TimestampNanosecondType};

    use super::*;

    #[test]
    fn negative_nanoseconds_from_the_cpp_writer_decode_as_signed() {
        let half_second_before = (-5 << 3) | 7;
        assert_eq!(
            decode_timestamp::<TimestampMicrosecondType>(0, -14_182_939, half_second_before)
                .unwrap(),
            -14_182_939_500_000
        );
        assert_eq!(
            decode_timestamp::<TimestampMicrosecondType>(0, 0, half_second_before).unwrap(),
            -500_000
        );
    }

    #[test]
    fn digits_below_the_unit_truncate_toward_zero() {
        assert_eq!(
            decode_timestamp::<TimestampMicrosecondType>(0, 1_704_067_200, 123_456_789 << 3)
                .unwrap(),
            1_704_067_200_123_456
        );
        assert_eq!(
            decode_timestamp::<TimestampMicrosecondType>(0, -14_182_939, -876_543_211 << 3)
                .unwrap(),
            -14_182_939_876_543
        );
    }

    #[test]
    fn out_of_range_values_fail_without_overflowing() {
        for value in [i64::MIN, i64::MAX] {
            assert!(decode_timestamp::<TimestampNanosecondType>(value, value, value).is_err());
            assert!(decode_timestamp::<TimestampMicrosecondType>(value, value, value).is_err());
        }
    }
}
