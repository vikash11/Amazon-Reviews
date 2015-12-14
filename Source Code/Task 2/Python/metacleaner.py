import json
from pprint import pprint

f = open("meta.json", 'w')
with open('umeta.json') as data_file:
    for line in data_file:
        data = json.loads(line)
        if 'related' in data:
            data.pop("related")
        if 'price' in data:
            data.pop("price")
        if 'imUrl' in data:       
            data.pop("imUrl")
        if 'salesRank' in data:
            data.pop("salesRank")
        if 'brand' in data:
            data.pop("brand")
        f.write(json.dumps(data) + '\n')
f.close()
