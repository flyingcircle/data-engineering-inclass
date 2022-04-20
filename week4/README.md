Response 1: Manually inputting a new set of data into a terminal. Sometimes errors would be thrown if the input didn’t pass all of the validation checks.

Response 2: Messing with data that has null values such as in SQL or Mongo. How should the data analyst handle the null value? What’s the difference between a null value and an empty value? These are rarely documented specifics so then it falls to the data analyst to make decisions about the meaning.

Response 3: No experience with data cleaning. More experience with having to fix the end of someone else’s pipeline or discarding invalid datapoints to present in a graph.

## Assertions

### Existence
- A unique vehicle ID exists for every crash ID.
- A unique participant ID exists for every crash ID.

### Limit
- Every crash occurred in 2019

### Intra-record
- If a latitude degrees field is populated then its latitude minutes field must also be populated

### Inter-record
- Every vehicle listed is part of a known crash
- Every crash ID is associated with at least one participant

### Summary
- The number of fatalities should not be > 5,000
- The number of total crashes should not be > 200,000

### Statistical Distribution
- The number of crashes tends to be higher in the beginning and end of the year than in the middle of the year.
- Plurality of crashes should happen in Washington, Multnomah, and Clackamas counties.

