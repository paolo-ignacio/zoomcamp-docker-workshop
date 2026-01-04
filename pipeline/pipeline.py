
import sys
import pandas as pd
# gets the argument from commandline
# example: you run with commandline: python pythonfilename.py randomNumber
# python pipelin.py 12 -> this results to ['pipeline.py' ,'12']


print(f"arguments: {sys.argv}")


#you can also do indexing
print(f"Month: {sys.argv[1]}")
# Input: python pipeline.py 12
# Output: Month: 12



df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")

print('hello pipeline')
