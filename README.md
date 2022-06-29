# --- DRAFT ---
# CSV ELT with AWS Glue

## Data pipeline flow
Get data into a queryable state as quickly as possible with the least amount of code.
 - good for devs: less code to write
 - good for ops: easier to detect and diagnose issues


## Problems
Problems with trying to do too much data processing outside of the 'query space':
* Glue crawler changes column/field types on different on subsequent runs, messing up schema projects onto the data
  files.  External table and S3 query errors or dropped records.

* fields lost from records due to type conversion errors.
  - initially defined as numeric, then contains string that can't be converted

* table definitions get corrupted on subsequent crawler runs
  - desired table def doesn't get new partition file metadata added
  - new spurious table definitions get added for individual files detected as different schemas


## Options
* define all the fields explicitly in the Glue catalog.  Don't use Glue crawler dynamic schema detection.
  + greatest control over unintended consequencies and side-effects
  + allows raw files to be processed 'as is'
  - loose advantage of dynamic schema detection.  Definitions have to be updated  in coordination with file changes
  - requires code definitions for every file schema
* custom serde to handle the CSV rules
  + allows raw files to be processed 'as is'
  - requires both custom coding and packaging
* convert to parquet with strings
  + provides a more robust format - column names and explicit data types
  + more performant and cost effective for later querying - compressed columnar file format
  - requires pre-processing of files to convert from raw format to parquet
  - parquet needs to be defined as string typed fields to avoid `int` conversions

## Senarios
### Scenario 1
1. csv file 0 contains a field detected as numeric by the crawler
2. csv file 1 contains a string value in the numeric typed field
3. field value silently dropped on read/query

### Scenario 2
1. csv file 0 contains a field detected as string by the crawler
2. csv file 2 contains an empty field and detected as int by the crawler
3. crawler creates a new table definition for the different file and misses partition detection.
