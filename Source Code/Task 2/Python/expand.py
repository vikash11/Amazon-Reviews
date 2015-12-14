import json
import gzip

def parse(path):
  g = gzip.open(path, 'r')
  for l in g:
    yield json.dumps(eval(l))

f = open("output.json", 'w')
for l in parse("input.json.gz"):
  f.write(l + '\n')
f.close()
