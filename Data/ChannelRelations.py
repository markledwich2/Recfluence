import os
import pandas as p

table = p.read_csv('.\\2.Analysis\\recommends.csv', delimiter=',')

#load channel data
allChannels = p.read_json('.\\2.Analysis\\channels.json')
allChannels[['id','title','subCount']].to_csv('.\\3.Vis\\Channels.csv')

# save to csv of relations
relations = table.groupby(['FromChannelTitle', 'ChannelTitle', 'DistanceFromSeed']).size()
relations.to_csv('.\\3.Vis\\ChannelRelations.csv', header=True)


