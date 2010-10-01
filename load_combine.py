from redis import Redis
r = Redis()

from pofs import POFS
p = POFS("iucr")

import simplejson

from time import sleep

def slow():
  for item in p.list_buckets():
    print "Adding %s to queue" % item
    push(item)

def queue_all():
  for item in p.list_buckets():
    r.lpush("sourcedatacombiner", simplejson.dumps({'receipt_id':"receipt:combine", 'agent':"Ben O'Steen", "id":item}))

def push(item):
  r.lpush("sourcedatacombiner", simplejson.dumps({'receipt_id':"receipt:combine", 'agent':"Ben O'Steen", "id":item}))
  line = ""
  while(not line):
    sleep(0.5)
    line = r.lpop("receipt:combine")
  jmsg = simplejson.loads(line)
  if not jmsg['success']:
    print "FAILED on %s" % item
    print line
  else:
    print "Successful"
