#!/usr/bin/python

from pprint import pprint
import sys

# doing the writes as one big block with an open and close per write
# causes a crash whlie opening the file, then doing all teh writes,
# then closing it does not crash

f = open('/mnt/testfile', 'w')
f.write('')
f.close()

sys.exit()

i = 1000
f = open('/mnt/testfile', 'w')
while i < 1000000:
    print str(i) + ": "
    buf = ''
    for j in range(i):
        buf += str(j)[-1]
        #print str(j)[-1],
    try:
        f.write(buf)
    except IOError as e:
        print('\nended on: ' + str(j))
        pprint(e)
        sys.exit(1)
#    finally:
#        f.close()
    i *= 10
f.write('\n')
f.close()
