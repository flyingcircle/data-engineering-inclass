import re
import pandas as pd
import numpy as np

df = pd.read_csv("books.csv")

df2 = df.drop(columns=['Edition Statement', 'Corporate Author', 
  'Corporate Contributors', 'Former owner', 'Engraver', 'Issuance type', 
  'Shelfmarks'])

print(df2["Date of Publication"].to_string())