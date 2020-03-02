import collect
import json
from bson import json_util

import re
tab = collect.Scrap("https://pt.wikipedia.org/wiki/Pato")
sla = tab.get_source_code().decode('utf-8')

# print(sla)

uow = {"source_code": sla, "url": "https://pt.wikipedia.org/wiki/Pato"}

plei = json.dumps(uow, default=json_util.default).encode()

xplok = json.loads(plei.decode())

print(re.sub(r"[\n\t]*","",xplok["source_code"]))

# data = json.loads(dataform)

# print(data)