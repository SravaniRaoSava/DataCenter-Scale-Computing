import pandas as pd


df = pd.read_csv("Source.csv")

df

df1=df.drop(['Serial Number'], axis=1)

df1


df1.to_csv('Target.csv')