#!/usr/bin/env python

from redisqueue import RedisQueue

from LogConfigParser import Config

import sys

from time import sleep
import simplejson

import rdflib
from rdflib import URIRef, ConjunctiveGraph as G, Literal, RDF

from xml.etree import ElementTree as ET
from xml.parsers.expat import ExpatError

import traceback, sys

from redis import Redis

# process version (for provenance)
PROCESS = "iucr_redis_tally"
PROCESS_VERSION = "0.1"

nss = {"prism": "http://prismstandard.org/namespaces/1.2/basic/",
       "dc": "http://purl.org/dc/elements/1.1/",
       "dcterms": "http://purl.org/dc/terms/",
       "ov":"http://open.vocab.org/terms/",
       "foaf":"http://xmlns.com/foaf/0.1/",
           }

from uuid import uuid4
import hashlib

if __name__ == "__main__":
  c = Config()
  redis_section = "redis"
  store_section = "ofs"
  worker_section = "worker_%s" % PROCESS
  worker_number = sys.argv[1]
  if len(sys.argv) == 3:
    if "redis_%s" % sys.argv[2] in c.sections():
      redis_section = "redis_%s" % sys.argv[2]

  rq = RedisQueue(c.get(worker_section, "listento"), "%s_%s" % (PROCESS, worker_number),
                  db=c.get(redis_section, "db"), 
                  host=c.get(redis_section, "host"), 
                  port=c.get(redis_section, "port")
                  )
  r = Redis(db=c.get(redis_section, "db"),
                  host=c.get(redis_section, "host"),
                  port=c.get(redis_section, "port"))

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

  idletime = 5

  while(True):
    line = rq.pop()
    if line:
      try:
        msg = simplejson.loads(line)
        # IUCr redis tallyer
        # Expects a simple message - just the 'id' to perform the combination task on and which 'agent' to do this on behalf of
        # Requires the object to hold the 'intermediary.rdf.xml'
        # (receipt queue id is also expected as for all tasks in 'receipt_id')
        agent = msg['agent']
        id = msg['id']
        receipt_id = msg['receipt_id']

        receipt_q = RedisQueue(receipt_id, "%s_%s" % (PROCESS, worker_number),
                  db=c.get(redis_section, "db"),
                  host=c.get(redis_section, "host"),
                  port=c.get(redis_section, "port")
                  )
        
      except ValueError:
        print "JSON msg couldn't be understood:"
        print "--REJECTED-START--"
        print line
        print "--REJECTED-END--"
        rq.task_complete()
        break
      except KeyError:
        print "Couldn't validate JSON msg - Required: agent, id, receipt_id"
        print "--MISSING-FIELDS-START--"
        print line
        print "--MISSING-FIELDS-END--"
        rq.task_complete()
        break
      
      # Start of actual parsing and combination
      # 1 - check id exists
      if not ofs.exists(id):
        receipt_q.push(simplejson.dumps({'success':False, 
                                         'comment':"%s does not exist" % id,
                                         'payload':msg,
                                         'process':PROCESS+PROCESS_VERSION,
                                         'id':id}))
        rq.task_complete()
        break

      # 2 - check for 'intermediary.rdf.xml' or fail and pass back receipt
      files = ofs.list_labels(id)
      if not ("intermediary.rdf.xml" in files):
        receipt_q.push(simplejson.dumps({'success':False, 
                                         'comment':"%s does not have intermediary.rdf.xml" % id,
                                         'payload':msg,
                                         'process':PROCESS+PROCESS_VERSION,
                                         'id':id}))
        rq.task_complete()
        break

      # 3 - get graph of RDF from the HTML
      int_rdf_text = ofs.get_stream(id, 'intermediary.rdf.html', as_stream=False)
      try:
        from xml.sax.xmlreader import InputSource
        from StringIO import StringIO
        class TextInputSource(InputSource, object):
          def __init__(self, text, system_id=None):
            super(TextInputSource, self).__init__(system_id)
            self.url = system_id
            file = StringIO(text)
            self.setByteStream(file)

          def __repr__(self):
            return self.url
     
        t = TextInputSource(int_rdf_text, system_id=id)
        t.setEncoding("UTF-8")
        g = G(identifier=id)
        g = g.parse(t, format="xml")
      except Exception, e:
        traceback.print_exc(file=sys.stdout)
        receipt_q.push(simplejson.dumps({'success':False,
                                         'comment':"%s - Unable to load intermediary.rdf.xml as an RDF graph - exception: %s" % (id, e),
                                         'payload':msg,
                                         'process':PROCESS+PROCESS_VERSION,
                                         'id':id}))

        rq.task_complete()
        break
      # 4 - vectorise key 'article' vocab
      try:
        rdfxml = html_g.serialize(format="xml")
        ofs.put_stream(id, "base.rdf.xml", rdfxml.encode("utf-8"), "full.html", agent, PROCESS+PROCESS_VERSION)
      except Exception, e:
        traceback.print_exc(file=sys.stdout)
        receipt_q.push(simplejson.dumps({'success':False,
                                         'comment':"%s - serialising 'base.rdf.xml' to disc failed - exception: %s" % (id, e),
                                         'payload':msg,
                                         'process':PROCESS+PROCESS_VERSION,
                                         'id':id}))
        rq.task_complete()
        break      

      # 5 - try to get extended author graph and serialize to 'extended_author.rdf.xml'
      try:
        xml_text = ofs.get_stream(id, 'data.cif.xml', as_stream=False)
        ext_authors_g = getauthorlist(xml_text, id, "data.cif.xml")
        rdfxml = ext_authors_g.serialize(format="xml")
        ofs.put_stream(id, "extended_author.rdf.xml", rdfxml.encode("utf-8"), "data.cif.xml", agent, PROCESS+PROCESS_VERSION)
        extended_success = True
      except Exception, e:
        #receipt_q.push(simplejson.dumps({'success':False,
        #                                 'comment':"%s - Error getting extended author information - exception: %s" % (id, e),
        #                                 'payload':msg,
        #                                 'process':PROCESS+PROCESS_VERSION,
        #                                 'id':id}))
        #rq.task_complete()
        print "Couldn't get extended author information for %s" % id
        traceback.print_exc(file=sys.stdout)
        extended_success = False

      if extended_success:
        # 6 - Munge graphs + mint author URIs + store intermediary rdf
        # 6.1 remove dc:creator's from html_g
        for s,p,o in html_g.triples((None, URIRef(nss['dc']+"creator"), None )):
          html_g.remove((s,p,o))
        # 6.2 manufacture URIs for authors (sha1 of iucr id + name)
        #   and insert into base graph as dcterms:creators with all the extra data
        for author in [o for s,p,o in ext_authors_g.triples((None, URIRef(nss['dcterms']+"creator"), None ))]:
          tuples = [(p,o) for s,p,o in ext_authors_g.triples(( author, None, None ))]
          name = [o for p,o in tuples if p == URIRef(nss['foaf']+"name")]
          if not name:
            print "Couldn't find a 'foaf:name' within:"
            print tuples
            name = uuid4().hex
          else:
            [name] = name
          sha1sum = hashlib.sha1()
          sha1sum.update(id+name)
          author_uri = URIRef("info:author:%s" % sha1sum.hexdigest())
          #add extracted authors to base graph
          html_g.add(( g_id, URIRef(nss['dcterms']+"creator"), author_uri ))
          html_g.add(( author_uri, RDF.type, URIRef(nss['foaf']+"Person") ))
          for p,o in tuples:
            html_g.add(( author_uri, p, o ))
      else:
        for s,p,o in html_g.triples((None, URIRef(nss['dc']+"creator"), None )):
          sha1sum = hashlib.sha1()
          sha1sum.update(id+str(o))
          author_uri = URIRef("info:author:%s" % sha1sum.hexdigest())
          html_g.remove((s,p,o))
          html_g.add(( g_id, URIRef(nss['dcterms']+"creator"), author_uri ))
          html_g.add(( author_uri, RDF.type, URIRef(nss['foaf']+"Person") ))
          html_g.add(( author_uri, URIRef(nss['foaf']+"name"), o ))

      # 6.3 serialize and store as "intermediary.rdf.xml"
      try:
        int_rdf = html_g.serialize(format="xml")
        ofs.put_stream(id, "intermediary.rdf.xml", int_rdf.encode("utf-8"), ["base.rdf.xml", "extended_author.rdf.xml"], agent, PROCESS+PROCESS_VERSION)
      except Exception, e:
        traceback.print_exc(file=sys.stdout)
        receipt_q.push(simplejson.dumps({'success':False,
                                         'comment':"%s - Failed to serialize and store intermediary rdf xml - exception: %s" % (id, e),
                                         'payload':msg,
                                         'process':PROCESS+PROCESS_VERSION,
                                         'id':id}))
        rq.task_complete()
        break
        
        
        
      receipt_q.push(simplejson.dumps({'success':True,
                                       'payload':msg,
                                       'process':PROCESS+PROCESS_VERSION,
                                       'id':id}))

      rq.task_complete()
    else:
      # ratelimit to stop it chewing through CPU cycles
      sleep(idletime)
