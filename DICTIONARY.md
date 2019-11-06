# Data

Details about the data are specified by DATA_PATH/data.yaml.  
Where DATA_PATH is an environment variable, which may be:

* `s3://username:password@bucket_name/path`
* `s3://bucket_name/path`
* `s3://bucket_name`
* a local path like: `./data`


This file is loaded the first time it is needed and then stored in memory.  The contents of `data.yaml` are stored as JSON in Elasticsearch in a single document of type `config` with id `1`.  

The version field of this document is checked at startup. If the new config has a new version, then we delete the whole index and re-index all of the files referred to in the `data.yaml` files section.

If no data.yml or data.yaml file is found, then all CSV files in `DATA_PATH` will be loaded, and all fields in their headers will be used.

For an example data file, visit https://collegescorecard.ed.gov/data/ and download the full data package. A data.yaml file will be included in the ZIP file download. 

# Dictionary Format

The data dictionary format may be (optionally) specified in the `data.yaml` file.  If unspecified, all columns are imported as strings.

## Simple Data Types

```
dictionary:
  name:
    source: COLUMN_NAME
    type: integer
    description: explanation of where this data comes from and its meaning
```

In the above example:
* `source:` is the name of the column in the csv. (This doesn't have to be all caps, we just find that to be common in government datasets.)
* `type:` may be `integer`,  `float`, `string`
* `description:` text description suitable for developer documentation or information provided to data analysts

## Calculated columns

Optionally, you can add "columns" by calculating fields at import based on multiple csv columns.  

```
academics.program.degree.health:
  calculate: CIP51ASSOC or CIP51BACHL
  type: integer
  description: Associate or Bachelor's degree in Health
```

Multiple operations are supported.  In the following example, if the columns `apples`, `oranges` and `plums` had a `0` value when there were none, and a `1` to represent if they were available, then these values could be combines with `or` to create a data field representing if any were true.

```
fruit:
  calculate: apples or oranges or plums
  type: integer
  description: is there any fruit available?
```
