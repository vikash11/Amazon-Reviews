import json
from pprint import pprint

f = open("reviews.json", 'w')
with open('ureviews.json') as data_file:
    for line in data_file:
        data = json.loads(line)
        if 'reviewerID' in data:
            data.pop("reviewerID")
        if 'reviewerName' in data:
            data.pop("reviewerName")
        if 'helpful' in data:       
            data.pop("helpful")
        if 'summary' in data:
            data.pop("summary")
        if 'reviewTime' in data:
            data.pop("reviewTime")
        f.write(json.dumps(data) + '\n')
f.close()
