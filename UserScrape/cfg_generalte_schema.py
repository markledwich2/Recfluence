import json
from cfg import Cfg

with open('./userscrape.schema.json', "w") as w:
    schemaTxt = json.dumps(Cfg.json_schema(), indent='  ')
    w.write(schemaTxt)
