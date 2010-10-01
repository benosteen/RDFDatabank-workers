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

# process version (for provenance)
PROCESS = "sourcedatacombiner"
PROCESS_VERSION = "0.1"

cif2rdflit = {'_publ_contact_author_name':('foaf','name'),
              '_publ_contact_author_address':('ov','recordedAddress'),
              '_publ_contact_author':('foaf','nick'),
              '_publ_contact_author_email':('foaf','mbox'),
              '_publ_author_name':('foaf','name'),
              '_publ_author_address':('ov','recordedAddress'),
             }

cif2rdfuri = {'_publ_contact_author_fax':('foaf','phone',"fax:+"),
              '_publ_contact_author_phone':('foaf','phone',"tel:+"),
              '_publ_author_fax':('foaf','phone',"fax:+"),
              '_publ_author_phone':('foaf','phone',"tel:+"),
             }


nss = {"prism": "http://prismstandard.org/namespaces/1.2/basic/",
       "dc": "http://purl.org/dc/elements/1.1/",
       "dcterms": "http://purl.org/dc/terms/",
       "ov":"http://open.vocab.org/terms/",
       "foaf":"http://xmlns.com/foaf/0.1/",
           }

from uuid import uuid4
import hashlib

def buildrdfgraphfromhtml(html, iucrid, filename):
  try:
    md_etree = ET.fromstring(html)
    data = {}
    for field in [x for x in md_etree.find("{http://www.w3.org/1999/xhtml}head").findall("{http://www.w3.org/1999/xhtml}meta") if not x.attrib['name'] in ["ROBOTS", "copyright"]]:
      name = field.attrib['name'].lower()
      if name == "keywords":
        name = "dc.subject"
      if not data.has_key(name):
        data[name] = []
      data[name].append(field.attrib['content'])

    if data.has_key("DC.identifier"):
      g_id = URIRef(data["DC.identifier"][0])
    elif data.has_key("dc.identifier"):
      g_id = URIRef(data["dc.identifier"][0])
    else:
      print "Couldn't find a dc:identifier field in this record %s:%s" % (iucrid, filename)
      g_id = URIRef("urn:uuid:%s" % uuid4().hex)
      print "Using %s for record %s:%s" % (g_id, iucrid,filename)

    if data.has_key("DC.link"):
      record_from = URIRef(data["DC.link"][0])
    if data.has_key("dc.link"):
      record_from = URIRef(data["dc.link"][0])
    else:
      print "Couldn't find a dc:link field in this record %s:%s" % (iucrid, filename)
      record_from = URIRef("urn:uuid:%s" % uuid4().hex)
      print "Using uuid:%s for record %s:%s" % (g_id, iucrid,filename)

    g = G(identifier=record_from)
    for ns in nss:
      g.bind(ns, nss[ns])

    for field in data:

      ns, pred = map(lambda x: x.lower(), field.split(".", 2))
      uri_pred = URIRef("%s%s" % (nss[ns], pred))
      for item in data[field]:
        if item.split(":")[0] in ["urn", "http", "ftp", "info", "doi"]:
          obj = URIRef(item)
        else:
          obj = Literal(item)
        g.add((g_id, uri_pred, obj))
    return g_id, g
  except ExpatError, e:
    print "Expat couldn't parse file: %s" % from_filename
    raise ExpatError

def getauthorlist(cifxml, iucrid, filename):
  cifdoc = ET.fromstring(cifxml)
  datablocks = [x for x in cifdoc.findall("datablock") if x.attrib['id'] != "I"]
  # Have to assume first non-'I' datablock is the one we need due to variable usage
  datablock = datablocks[0]
  # get main data from this block and push the data into a simple graph with 
  # "info:author#primary" as the key author, and 'info:work' as the fake node for the article.
  g = G(identifier=iucrid)
  for ns in nss:
    g.bind(ns, nss[ns])
  
  data = {}
  for item in datablock.findall("item"):
    contents = item.text.strip()
    if contents:
      data[item.attrib['name']] = item.text.strip()

  for key in cif2rdflit.keys():
    if data.has_key(key):
      g.add((URIRef("info:author#1"), URIRef(nss[cif2rdflit[key][0]]+cif2rdflit[key][1]), Literal(data[key]) ))

  for key in cif2rdfuri.keys():
    if data.has_key(key):
      nospaces = "".join(data[key].split(" "))
      g.add((URIRef("info:author#1"), URIRef(nss[cif2rdfuri[key][0]]+cif2rdfuri[key][1]), URIRef(cif2rdfuri[key][2]+nospaces) ))

  g.add(( URIRef("info:work"), URIRef(nss['dcterms']+"creator"), URIRef("info:author#1") ))
  primaryauthor = data.get('_publ_contact_author_name', "")
 
  priauthoraddress = data.get('_publ_contact_author_address', "")
  
  # get other authors

  author_no = 2
  # now for the authorlist
  for row in datablock.find("loop").findall("row"):
    fields = datablock.find("loop").attrib['names'].split(" ")
    values = [x.text.strip() for x in row.findall("cell")]
    for loop_field_index in xrange(len(fields)):
      if fields[loop_field_index] == "_publ_author_name" and values[loop_field_index] == primaryauthor:
        author_uri = URIRef("info:author#1")
      else:
        author_uri = URIRef("info:author#%s" % author_no)
        author_no = author_no + 1
      g.add(( URIRef("info:work"), URIRef(nss['dcterms']+"creator"), author_uri ))
      for key in cif2rdflit.keys():
        if key in fields:
          value = values[fields.index(key)]
          g.add((author_uri, URIRef(nss[cif2rdflit[key][0]]+cif2rdflit[key][1]), Literal(value) ))

      for key in cif2rdfuri.keys():
        if key in fields:
          value = values[fields.index(key)]
          if value:
            nospaces = "".join(value.split(" "))
            g.add(( author_uri, URIRef(nss[cif2rdfuri[key][0]]+cif2rdfuri[key][1]), URIRef(cif2rdfuri[key][2]+nospaces) ))
        
  return g
  

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
        # IUCr source data combiner
        # Expects a simple message - just the 'id' to perform the combination task on and which 'agent' to do this on behalf of
        # Requires the object to hold a 'full.html' and 'data.cif.xml' from lensfield2
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

      # 2 - check for 'full.html' and 'data.cif.xml' or fail and pass back receipt
      files = ofs.list_labels(id)
      if not ("full.html" in files and "data.cif.xml" in files):
        receipt_q.push(simplejson.dumps({'success':False, 
                                         'comment':"%s does not have both full.html and data.cif.xml" % id,
                                         'payload':msg,
                                         'process':PROCESS+PROCESS_VERSION,
                                         'id':id}))
        rq.task_complete()
        break

      # 3 - get graph of RDF from the HTML
      html_text = ofs.get_stream(id, 'full.html', as_stream=False)
      try:
        g_id, html_g = buildrdfgraphfromhtml(html_text, id, 'full.html') 
      except Exception, e:
        traceback.print_exc(file=sys.stdout)
        receipt_q.push(simplejson.dumps({'success':False,
                                         'comment':"%s - full.html to graph failed - exception: %s" % (id, e),
                                         'payload':msg,
                                         'process':PROCESS+PROCESS_VERSION,
                                         'id':id}))

        rq.task_complete()
        break
      # 4 - store RDF as 'base.rdf.xml'
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
