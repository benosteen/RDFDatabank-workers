#!/usr/bin/env python

from redisqueue import RedisQueue

from LogConfigParser import Config

import sys

from time import sleep

from rdflib import URIRef

from recordsilo import Granary

from solr import SolrConnection

import simplejson

DB_ROOT = "/opt/rdfdatabank/src"

class NoSuchSilo(Exception):
  pass

def gather_document(silo_name, item):
  graph = item.get_graph()
  document = {'id':item.item_id, 'silo':silo_name}
  for (_,p,o) in graph.triples((URIRef(item.uri), None, None)):
    print p,o
    if p.startswith("http://purl.org/dc/terms/"):
      field = p[len("http://purl.org/dc/terms/"):].encode("utf-8")
      if not document.has_key(field):
        document[field] = []
      print "Adding %s to %s" % (o, field)
      document[field].append(unicode(o).encode("utf-8"))
    else:
      if not document.has_key('text'):
        document['text'] = []
      print "Adding %s to %s - (from %s)" % (o, "text", p)
      document['text'].append(unicode(o).encode("utf-8"))
  print document
  return document

if __name__ == "__main__":
  c = Config()
  redis_section = "redis"
  worker_section = "worker_solr"
  worker_number = sys.argv[1]
  if len(sys.argv) == 3:
    if "redis_%s" % sys.argv[2] in c.sections():
      redis_section = "redis_%s" % sys.argv[2]

  rq = RedisQueue(c.get(worker_section, "listento"), "solr_%s" % worker_number,
                  db=c.get(redis_section, "db"), 
                  host=c.get(redis_section, "host"), 
                  port=c.get(redis_section, "port")
                  )
  rdfdb_config = Config("%s/production.ini" % DB_ROOT)
  granary_root = rdfdb_config.get("app:main", "granary.store", 0, {'here':DB_ROOT})
  
  g = Granary(granary_root)

  solr = SolrConnection(c.get(worker_section, "solrurl"))

  idletime = 2

  while(True):
    line = rq.pop()
    if line:
      try:
        msg = simplejson.loads(line)
        # solr switch
        silo_name = msg['silo']
        if silo_name not in g.silos:
          g._register_silos()
          if silo_name not in g.silos:
            raise NoSuchSilo
        s = g.get_rdf_silo(silo_name)
        if msg['type'] == "c" or msg['type'] == "u":
          # Creation
          itemid = msg.get('id')
          print "Got creation message on id:%s in silo:%s" % (itemid, silo_name)
          if itemid and s.exists(itemid):
            # Embargo flag?
            item = s.get_item(itemid)
            if item.metadata.get('embargoed', True) not in ['true','True', True]:
              solr_doc = gather_document(silo_name, item)
              solr.add(_commit=True, **solr_doc)
          rq.task_complete()
        elif msg['type'] == "d":
          # Deletion
          itemid = msg.get('id')
          if itemid and s.exists(itemid):
            solr.delete(itemid)
            solr.commit()
          rq.task_complete()
        elif msg['type'] == "embargo":
          itemid = msg.get('id')
          if itemid and s.exists(itemid):
            if msg['embargoed'] in ['false', 'False', 0, False]:
              # Embargo removed: update solr
              item = s.get_item(itemid)
              solr_doc = gather_document(silo_name, item)
              solr.add(_commit=True, **solr_doc)
            else:
              solr.delete(itemid)
              solr.commit()
          rq.task_complete()
      except NoSuchSilo:
        print "ERROR: Silo doesn't exist %s" % silo_name
        print line
        rq.task_complete()
    else:
      # ratelimit to stop it chewing through CPU cycles
      sleep(idletime)
