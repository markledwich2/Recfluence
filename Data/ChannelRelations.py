import os
import pandas as p

recommends = p.read_csv('.\\2.Analysis\\recommends.csv', delimiter=',')
channels = p.read_json('.\\2.Analysis\\channels.json')


channels = channels[['id','title','subCount']].query('subCount > 10000')
channels.sort_values('subCount', ascending=False)

# save to csv of relations
recommends = recommends.groupby(['FromChannelTitle', 'ChannelTitle', 'FromChannelId', 'ChannelId', 'DistanceFromSeed']).size().to_frame('Size')
chIds = channels['id']
recommends[recommends['FromChannelId'].isin(chIds)].count()
recommends

channels.to_csv('.\\3.Vis\\Channels.csv')
recommends.to_csv('.\\3.Vis\\ChannelRelations.csv', header=True)


