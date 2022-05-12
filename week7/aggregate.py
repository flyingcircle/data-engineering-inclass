import pandas as pd
import numpy as np

covid_df = None
with open("COVID_county_data.csv") as f:
  covid_df = pd.read_csv(f)
covid_df

acs_df = None
with open("acs2017_census_tract_data.csv") as f:
  acs_df = pd.read_csv(f)
acs_df = acs_df[['State', 'County', 'TotalPop', 'Poverty', 'IncomePerCap']]

acs_df = acs_df[acs_df['TotalPop'] > 0]
acs_df = acs_df[acs_df['Poverty'].notna()]
acs_df = acs_df[acs_df['IncomePerCap'].notna()]

weighted_acs = lambda x: np.average(x, weights=acs_df.loc[x.index, "TotalPop"])

acs_df2 = pd.DataFrame(acs_df.groupby(['State', 'County']).agg(Population=('TotalPop','sum'),
  Poverty=('Poverty',weighted_acs),
  IncomePerCapita=('IncomePerCap',weighted_acs))).reset_index()

'''
I got farther in the notebook. Please review that code instead.
'''

