import numpy as np
import pandas as pd

df = pd.read_csv('ytapi_default.csv')

print(df[['textDisplay', 'comment_id']])