//! Read a GRIB1 file and search for data based on parameter and level values, and decode the data. Or extract the complete subfile so it can be saved to a separate file.
//! Currently only the Code10 (RotatedLatLon) data type is supported.

use bitstream_io::{BigEndian, BitRead, BitReader};
use error::Grib1Error;
use std::io::Cursor;
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};

pub mod error;

/// Reader of grib version 1 files
pub struct Grib1Reader {
    pub reader: BufReader<File>,
}

#[derive(Debug)]
/// Grib file representation
pub struct Grib {
    pub length: u64,
    pub pds: PDS,
    pub gds: Option<GDS>,
    pub bds: Option<BDS>,
}

#[derive(Debug, Clone, Copy)]
pub struct RotatedLatLon {
    pub number_of_lat_values: u16,
    pub number_of_lon_values: u16,
    pub latitude_of_first_grid_point: f32,
    pub longitude_of_first_grid_point: f32,
    pub latitude_of_last_grid_point: f32,
    pub longitude_of_last_grid_point: f32,
    pub latitude_of_southern_pole: f32,
    pub longitude_of_southern_pole: f32,
}

#[derive(Debug, Clone, Copy)]
/// List of data types the library supports (is able to decode)
pub enum DataRepresentation {
    Unhandled,
    RotatedLatLon(RotatedLatLon),
}

#[derive(Debug)]
enum GribResult {
    Length(u64),
    Grib(Grib),
}

#[derive(Debug, Clone)]
/// Grid description section
pub struct GDS {
    pub number_of_vertical_coordinate_values: u8,
    pub pvl_location: u8,
    pub data_representation_type: u8,
    pub data: DataRepresentation,
}

#[derive(Debug, Clone)]
/// Product definition section
pub struct PDS {
    pub parameter_table_version_number: u8,
    pub identification_of_center: u8,
    pub generating_process_id_number: u8,
    pub grid_identification: u8,
    pub flag_specifying_the_presence_or_absence_of_a_gds_or_a_bms: u8,
    pub indicator_of_parameter_and_units: u8,
    pub indicator_of_type_of_level_or_layer: u8,
    pub level_or_layer_value: u16,
    pub year: u8,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub forecast_time_unit: u8,
    pub p1_period_of_time: u8,
    pub p2_period_of_time: u8,
    pub time_range_indicator: u8,
    pub number_missing_from_averages_or_accumulations: u8,
    pub century_of_initial_reference_time: u8,
    pub identification_of_sub_center: u8,
    pub decimal_scale_factor: i16,
}

impl PDS {
    pub fn has_gds(&self) -> bool {
        self.flag_specifying_the_presence_or_absence_of_a_gds_or_a_bms & 128 > 0
    }

    pub fn has_bmp(&self) -> bool {
        self.flag_specifying_the_presence_or_absence_of_a_gds_or_a_bms & 64 > 0
    }
}

#[derive(Debug)]
///Bit-map section
pub struct Bitmap {
    pub number_of_unused_bits_at_end_of_section3: u8,
    pub table_reference: u16,
}

#[derive(Debug, Clone)]
/// Binary data section
pub struct BDS {
    pub data_flag: u8,
    pub binary_scale_factor: i16,
    pub reference_value: f32,
    pub bits_per_value: u8,
    pub data: Vec<f32>,
}

#[derive(Debug)]
pub struct SearchParams {
    pub param: u32,
    pub level: u32,
}

impl Grib1Reader {
    /// Create a new instance of the GRIB1 reader by specifying the BufReader wrapping the file to read
    pub fn new(buf_reader: BufReader<File>) -> Grib1Reader {
        Grib1Reader { reader: buf_reader }
    }

    /// Read the file looking for data matching the specified search parameters and returning the decoded result
    pub async fn read(&mut self, search: Vec<SearchParams>) -> Result<Vec<Grib>, Grib1Error> {
        let mut offset = 0;
        let mut result = vec![];

        // We need to know how large the file is, so we know when to stop
        let length = self.reader.seek(SeekFrom::End(0)).await?;

        while offset < length {
            self.reader.seek(SeekFrom::Start(offset)).await?;

            let grib_result = self.read_grib(&search, true).await?;
            let length = match grib_result {
                GribResult::Grib(grib) => {
                    let length = grib.length;
                    result.push(grib);

                    length
                }
                GribResult::Length(length) => length,
            };

            offset += length;
        }

        Ok(result)
    }

    /// Read the file looking for data matching the specified search parameters and returning the binary blob representing the file
    pub async fn read_binary(&mut self, search: Vec<SearchParams>) -> Result<Vec<u8>, Grib1Error> {
        let mut offset = 0;
        let mut result = vec![];

        // We need to know how large the file is, so we know when to stop
        let length = self.reader.seek(SeekFrom::End(0)).await?;

        while offset < length {
            self.reader.seek(SeekFrom::Start(offset)).await?;

            let grib_result = self.read_grib(&search, false).await?;

            let length = match grib_result {
                GribResult::Grib(grib) => {
                    // Go back to the start of the block and read all of it into a buffer we can return
                    self.reader.seek(SeekFrom::Start(offset)).await?;

                    let mut buffer = vec![0; grib.length as usize];
                    self.reader.read_exact(&mut buffer).await?;
                    result.append(&mut buffer);

                    grib.length
                }
                GribResult::Length(length) => length,
            };

            offset += length;
        }

        Ok(result)
    }

    async fn read_grib(&mut self, search_list: &Vec<SearchParams>, read_bds: bool) -> Result<GribResult, Grib1Error> {
        // The first 8 bytes describes the header of the grib1 file
        let mut buffer = [0; 8];
        let _ = self.reader.read(&mut buffer).await?;

        // Look for the letters GRIB that indicate this is indeed the kind of file we can read
        let header: [u8; 4] = [0x47, 0x52, 0x49, 0x42];
        if header != buffer[0..4] {
            return Err(Grib1Error::WrongHeader);
        }

        // We use the length of the section to skip to the next one if we aren't interested in it
        let length_of_grib_section = read_u24_be(&buffer[4..]);

        // Make sure this is indeed a version we can understand
        let version = buffer[7];
        if version != 1 {
            return Err(Grib1Error::WrongVersion(version));
        }

        let pds = self.read_pds().await?;

        let mut result = Grib {
            length: length_of_grib_section as u64,
            pds,
            gds: None,
            bds: None,
        };

        let mut number_of_lat_values = 0;
        let mut number_of_lon_values = 0;
        if result.pds.has_gds() {
            let gds = self.read_gds().await?;

            // If we found a rotated lat/lon scheme grab the values we need
            if let DataRepresentation::RotatedLatLon(value) = gds.data {
                number_of_lat_values = value.number_of_lat_values;
                number_of_lon_values = value.number_of_lon_values;
            }
            result.gds = Some(gds);
        }

        if result.pds.has_bmp() {
            // The data this library is written for doesn't contain bitmaps, so this is more here for show.
            let _bitmap = self.read_bitmap().await?;
        }

        // Check to see if this is the data we are interested in
        for seach_item in search_list {
            if result.pds.indicator_of_parameter_and_units == seach_item.param as u8 && result.pds.level_or_layer_value == seach_item.level as u16 {
                // If we are just interested in the binary blob we don't need to read and unpack the actual contained data
                if read_bds {
                    let bds = self.read_bds(number_of_lat_values as usize * number_of_lon_values as usize).await?;
                    result.bds = Some(bds);
                }

                return Ok(GribResult::Grib(result));
            }
        }

        Ok(GribResult::Length(length_of_grib_section as u64))
    }

    async fn read_gds(&mut self) -> Result<GDS, Grib1Error> {
        let len = self.get_length().await?;

        let mut buffer = vec![0; len];
        self.reader.read_exact(&mut buffer).await?;

        let data_representation_type = buffer[5];

        let mut data = DataRepresentation::Unhandled;
        if data_representation_type == 10 {
            data = DataRepresentation::RotatedLatLon(RotatedLatLon {
                number_of_lat_values: read_u16_be(&buffer[6..]),
                number_of_lon_values: read_u16_be(&buffer[8..]),
                latitude_of_first_grid_point: read_i24_be(&buffer[10..]) as f32 * 0.001,
                longitude_of_first_grid_point: read_i24_be(&buffer[13..]) as f32 * 0.001,
                latitude_of_last_grid_point: read_i24_be(&buffer[17..]) as f32 * 0.001,
                longitude_of_last_grid_point: read_i24_be(&buffer[20..]) as f32 * 0.001,
                latitude_of_southern_pole: read_i24_be(&buffer[32..]) as f32 * 0.001,
                longitude_of_southern_pole: read_i24_be(&buffer[35..]) as f32 * 0.001,
            });
        }

        Ok(GDS {
            number_of_vertical_coordinate_values: buffer[3],
            pvl_location: buffer[4],
            data_representation_type: buffer[5],
            data,
        })
    }

    async fn read_pds(&mut self) -> Result<PDS, Grib1Error> {
        let len = self.get_length().await?;

        let mut buffer = vec![0; len];
        self.reader.read_exact(&mut buffer).await?;

        Ok(PDS {
            parameter_table_version_number: buffer[3],
            identification_of_center: buffer[4],
            generating_process_id_number: buffer[5],
            grid_identification: buffer[6],
            flag_specifying_the_presence_or_absence_of_a_gds_or_a_bms: buffer[7],
            indicator_of_parameter_and_units: buffer[8],
            indicator_of_type_of_level_or_layer: buffer[9],
            level_or_layer_value: read_u16_be(&buffer[10..]),
            year: buffer[12],
            month: buffer[13],
            day: buffer[14],
            hour: buffer[15],
            minute: buffer[16],
            forecast_time_unit: buffer[17],
            p1_period_of_time: buffer[18],
            p2_period_of_time: buffer[19],
            time_range_indicator: buffer[20],
            number_missing_from_averages_or_accumulations: buffer[23],
            century_of_initial_reference_time: buffer[24],
            identification_of_sub_center: buffer[25],
            decimal_scale_factor: read_i16_be(&buffer[26..]),
        })
    }

    async fn read_bitmap(&mut self) -> Result<Bitmap, Grib1Error> {
        let len = self.get_length().await?;
        let mut buffer = vec![0; len];
        self.reader.read_exact(&mut buffer).await?;

        Ok(Bitmap {
            number_of_unused_bits_at_end_of_section3: buffer[3],
            table_reference: read_u16_be(&buffer[4..]),
        })
    }

    async fn read_bds(&mut self, number_of_data_points: usize) -> Result<BDS, Grib1Error> {
        let len = self.get_length().await?;
        let mut buffer = vec![0; len];
        self.reader.read_exact(&mut buffer).await?;

        let binary_scale = read_i16_be(&buffer[4..]);
        let ref_value = read_f32_ibm(&buffer[6..]);
        let bit_count = buffer[10];

        let mut r = BitReader::endian(Cursor::new(&buffer[11..]), BigEndian);
        let mut result = vec![];
        let mut iterations = 0;
        let base: f32 = 2.0;
        let factor = base.powf(binary_scale as f32);

        // Convert all the packed data into f32 values
        while iterations < number_of_data_points {
            if let Ok(x) = r.read::<u32>(bit_count as u32) {
                let y = ref_value + (x as f32) * factor;
                result.push(y);
            } else {
                return Err(Grib1Error::DataDecodeFailed);
            }
            iterations += 1;
        }

        Ok(BDS {
            data_flag: buffer[3],
            binary_scale_factor: binary_scale,
            reference_value: ref_value,
            bits_per_value: bit_count,
            data: result,
        })
    }

    async fn get_length(&mut self) -> Result<usize, Grib1Error> {
        // The header might be of variable length, so we read the length first, and then reset the position so the offsets in the documentation still fits
        let mut buffer = [0; 3];
        self.reader.read_exact(&mut buffer).await?;
        let len = read_u24_be(&buffer[..]) as usize;
        self.reader.seek(SeekFrom::Current(-3)).await?;

        Ok(len)
    }
}

//
// Utility funtions to convert slices of memory into the value types we want
//

fn read_f32_ibm(data: &[u8]) -> f32 {
    let sign = if (data[0] & 0x80) > 0 { -1.0 } else { 1.0 };
    let a = (data[0] & 0x7f) as i32;
    let b = (((data[1] as i32) << 16) + ((data[2] as i32) << 8) + data[3] as i32) as f32;

    sign * 2.0f32.powi(-24) * b * 16.0f32.powi(a - 64)
}

fn read_i16_be(array: &[u8]) -> i16 {
    let mut val = (array[1] as i16) + (((array[0] & 127) as i16) << 8);
    if array[0] & 0x80 > 0 {
        val = -val;
    }
    val
}

fn read_i24_be(array: &[u8]) -> i32 {
    let mut val = (array[2] as i32) + ((array[1] as i32) << 8) + (((array[0] & 127) as i32) << 16);
    if array[0] & 0x80 > 0 {
        val = -val;
    }
    val
}

fn read_u16_be(array: &[u8]) -> u16 {
    (array[1] as u16) + ((array[0] as u16) << 8)
}

fn read_u24_be(array: &[u8]) -> u32 {
    (array[2] as u32) + ((array[1] as u32) << 8) + ((array[0] as u32) << 16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_test() -> Result<(), Grib1Error> {
        let f = File::open("data/sample.grib").await?;

        let mut reader = Grib1Reader::new(BufReader::new(f));
        let result = reader.read(vec![SearchParams { param: 33, level: 700 }, SearchParams { param: 34, level: 700 }]).await?;

        assert_eq!(2, result.len());

        assert_eq!(result[0].pds.indicator_of_parameter_and_units, 33);
        assert_eq!(result[0].pds.level_or_layer_value, 700);

        assert_eq!(result[1].pds.indicator_of_parameter_and_units, 34);
        assert_eq!(result[1].pds.level_or_layer_value, 700);

        println!("Results:");
        for grib in result {
            println!("{:#?}", &grib.pds);
            if let Some(gds) = grib.gds {
                println!("{:#?}", &gds);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn read_binary_test() -> Result<(), Grib1Error> {
        let f = File::open("data/sample.grib").await?;

        let mut reader = Grib1Reader::new(BufReader::new(f));
        let result = reader.read_binary(vec![SearchParams { param: 33, level: 700 }]).await?;

        println!("Result length: {}", result.len());
        assert_eq!(2542704, result.len());

        Ok(())
    }
}
