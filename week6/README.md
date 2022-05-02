

## 

Do you have any experience with ingesting bulk data into a database? If yes, then describe your experience, especially what method was used to input the data into the database. If no, then describe how you might ingest daily incremental breadcrumb data for your class project.

Group answers: No I never have. Even considering "large", I can't remember a time where I ever transferred large amounts of data into a db. We can just ingest the breadcrumb data by chunking the data into the db rather than one row at a time.

My answers: No I have never transferred large chunks of data into a database. I can only recall working with single rows or small chunks at a time. I believe the for the breadcrumb data, the pandas dataframe.to_sql will load the data into a database in large batches, so I don't believe this should be too much of an issue.

| Method | Time to load part1 | Time to load part2 |
| ------ | ------------------ | ------------------ |
| C. Simple inserts | 0:36.28 | 0:35.85 |
| D. Drop Indexes and Constraints | 0:35.21 | 0:35.79 |
| E. Disable Autocommit | 0:06.1 | 0:05.4 |
| F. Use UNLOGGED table | 0:08.98 | 0:08.46 |
| G. Temp Table with memory tuning | | |
| H. Batching | | |
| I. copy_from | | |

