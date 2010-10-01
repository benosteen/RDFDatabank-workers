#!/usr/bin/env python

from redisqueue import RedisQueue

from LogConfigParser import Config

import sys

from time import sleep

from urllib import urlencode
from urllib2 import urlparse
from httplib2 import Http

import simplejson

# process version (for provenance)
PROCESS = "HTTP_loader"
PROCESS_VERSION = "0.1"
 
h = Http()

class FailedDownload(Exception):
  pass

class ServerFailOnDownload(Exception):
  pass

def gather_document(url):
  (resp, content) = h.request(url)
  if resp.status in [200,204]: 
    return resp, content
  elif resp.status in [400,404]:
    raise FailedDownload
  elif resp.status in [500, 502, 504]:
    raise ServerFailOnDownload

if __name__ == "__main__":
  c = Config()
  redis_section = "redis"
  store_section = "ofs"
  worker_section = "worker_http"
  worker_number = sys.argv[1]
  if len(sys.argv) == 3:
    if "redis_%s" % sys.argv[2] in c.sections():
      redis_section = "redis_%s" % sys.argv[2]

  rq = RedisQueue(c.get(worker_section, "listento"), "httploader_%s" % worker_number,
                  db=c.get(redis_section, "db"), 
                  host=c.get(redis_section, "host"), 
                  port=c.get(redis_section, "port")
                  )
  
  ofs_module = c.get(store_section, "ofs_client_module")
  ofs_root = c.get(store_section, "ofs_root")
  ofs_class = c.get(store_section, "ofs_class")
  try:
    OFS_mod = __import__(ofs_module)
    OFS_impl = getattr(OFS_mod, ofs_class)
  except ImportError:
    print "Module %s could not be imported" % ofs_module_root
    sys.exit(2) 
  except AttributeError:
    print "Class %s could not be imported from module %s" % (ofs_class, ofs_module)
    sys.exit(2) 
  
  ofs = OFS_impl(ofs_root)

  idletime = 2

  while(True):
    line = rq.pop()
    if line:
      try:
        msg = simplejson.loads(line)
        # HTTP loader - looks for a dict
        #  - 'url', 'id', 'agent'
        try:
          resp, doc = gather_document(msg['url'])
        except ServerFailOnDownload, e:
          print "Server returned an error when attempting to get %s" % msg['url']
          print "Requeueing"
          rq.task_failed()
          print repr(e)

        parsed_url = urlparse.urlparse(msg['url'])
        name = parsed_url.path.split("/")[-1]
        if not name or name == "/":
          name = "ROOTPAGE"
        if 'filename' in msg and msg.get('filename'):
          name = msg['filename']
        ofs.put_stream(msg['id'], name, doc, msg['url'], msg['agent'], "%s-%s" % (PROCESS, PROCESS_VERSION), params = {'http_headers':dict(resp)})
        rq.task_complete()
#      except TypeError:
#        print "HTTP_loader requires 'url', 'id' and 'agent'"
#        print "REJECTED"
#        print msg
#        print "---"
      except ValueError:
        print "JSON msg couldn't be understood:"
        print "REJECTED"
        print msg
        print "---"
    else:
      # ratelimit to stop it chewing through CPU cycles
      sleep(idletime)
