import pandas
import pandera as pa
from pandera import Column, Check


with open("highway_data.csv") as f:
  data = pandas.read_csv(f)

crash_df = data[data["Record Type"] == 1]
vehicle_df = data[data["Record Type"] == 2]
people_df = data[data["Record Type"] == 3]

crash_schema = pa.DataFrameSchema(
  {
    "Crash ID": Column(int, unique=True), 
    "Record Type": Column(int, Check.in_range(1,3)),
    "Crash Year": Column(int, Check.eq(2019), coerce=True)
  }
)
crash_schema.validate(crash_df)

vehicle_schema = pa.DataFrameSchema(
  unique=["Crash ID", "Vehicle ID"]
)

vehicle_schema.validate(vehicle_df)

people_schema = pa.DataFrameSchema(
  unique=["Crash ID", "Participant ID"]
)

people_schema.validate(people_df)

# If a latitude degrees field is populated then its latitude minutes field must also be populated
assert all([x is None or x >= 0 and y >= 0 
  for (x,y) in list(zip(crash_df["Latitude Degrees"],crash_df["Latitude Minutes"]))])

# Every vehicle listed is part of a known crash
assert all([x in crash_df["Crash ID"].values for x in vehicle_df["Crash ID"].values])

# Every crash ID is associated with at least one participant
assert all([x in people_df["Crash ID"].values for x in crash_df["Crash ID"].values])

# The number of fatalities should not be > 5,000
assert sum(crash_df["Total Fatality Count"].values) < 5000

# The number of total crashes should not be > 200,000
assert len(crash_df) < 200_000

# Plurality of crashes should happen in Washington, Multnomah, and Clackamas counties.
counties = list(crash_df["County Code"].values)
assert (counties.count(3) + counties.count(26) + counties.count(34)) / len(counties) > .3

