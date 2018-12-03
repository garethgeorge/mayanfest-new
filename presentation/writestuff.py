import time 

import os

import sys

filename = sys.argv[1]
bytes = int(sys.argv[2])

curtime = time.time()
with open(filename, "w") as f:
    f.write("a" * bytes)

print("%d, %d" % (bytes, time.time() - curtime))
