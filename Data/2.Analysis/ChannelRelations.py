#%%

import pandas as p

fileName = 'recommends.csv'
table = p.read_csv(fileName, delimiter=',')

#%%
relations = table.groupby(['FromChannelTitle', 'ChannelTitle', 'DistanceFromSeed']).size()
relations.to_csv('.\\Data\\3.Vis\\ChannelRelations.csv', header=True)