import sys
sys.path.append("..")

from semlog_mongo.mongo import *
from semlog_mongo.utils import *
from pymongo import MongoClient

import pandas as pd


m=MongoDB([['VisReplayMeta','1']])

df=m.new_entity_search(id_list=['MPWineBottle'])
download_images('localhost','T','t',df)
df.to_csv("new.csv",index=False)
print(df.shape)
