# GRIB1 Reader

Read a GRIB1 file and search for data based on parameter and level values, and decode the data. Or extract the complete subfile so it can be saved to a separate file.

Currently only the Code10 (RotatedLatLon) data type is supported.

# Usage
Add this to your Cargo.toml:

```
[dependencies]
grib1_reader = "0.1.0"
```
and this to your source code:

```
use grib1_reader::{Grib1Reader, SearchParams};
```
# Example

```
let file = File::open("data/sample.grib").await?;
let mut reader = Grib1Reader::new(BufReader::new(file));
let result = reader.read(vec![SearchParams { param: 33, level: 700 }]).await?;

println!("Results:");
for grib in result {
    println!("{:#?}", &grib.pds);
    if let Some(gds) = grib.gds {
        println!("{:#?}", &gds);
    }
}
```