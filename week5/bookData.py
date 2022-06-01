import re
import pandas as pd
import numpy as np

df = pd.read_csv("books.csv")

df2 = df.drop(columns=['Edition Statement', 'Corporate Author', 
  'Corporate Contributors', 'Former owner', 'Engraver', 'Issuance type', 
  'Shelfmarks'])

print(df2["Date of Publication"].to_string())

df3 = df2.copy()
# TODO: This regex isn't doing everything needed.
df3["Date of Publication"] = df3["Date of Publication"].str.extract(r'(\d{4})')
df3["Date of Publication"] = pd.to_numeric(df3["Date of Publication"], errors='coerce')
df3["Date of Publication"].values

df4 = df3.copy()
# TODO: again not a perfect regex
df4["Place of Publication"] = df4["Place of Publication"].str.extract(r'([a-zA-Z]+)')
df4["Place of Publication"] = df4["Place of Publication"].replace(np.NAN, "unknown")
df4["Place of Publication"].str.strip()

with open("uniplaces.txt") as file:
  unidf = pd.DataFrame([line.rstrip() for line in file])
unidf

import re
stateNameRegex = r'(.+)\[edit\]'
infoRegex = r'(.+)\s\((.+)\)(\[\d*\])*'
currentState = ""
def format(text):
  if re.search(stateNameRegex, text):
    global currentState
    currentState = re.match(stateNameRegex, text).group(1)
  else:
    m = re.match(infoRegex, text)
    if m is not None:
      return [currentState, m.group(1), m.group(2)]

unidf.applymap(format).dropna()
# This doesn't split out the universities, but there are
# a lot of text errors in the university lists.
