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
* custom serde to handle the CSV rules
* convert to parquet with strings

## Scenarios
* 