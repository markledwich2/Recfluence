import json
from cfg import Cfg

## run this file to generate a new schema

with open('./userscrape.schema.json', "w") as w:
    schemaTxt = json.dumps(Cfg.json_schema(), indent='  ')
    w.write(schemaTxt)