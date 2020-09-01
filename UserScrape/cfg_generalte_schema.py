import json
from userscrape.cfg import Cfg

with open('userscrape.schema.json', "w") as w:
    jSchema = Cfg.json_schema()
    schemaTxt = json.dumps(jSchema, indent='  ')
    w.write(schemaTxt)
