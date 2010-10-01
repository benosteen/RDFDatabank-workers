#!/usr/bin/env python

import sys, os

from xml.etree import ElementTree as ET

from xml.parsers.expat import ExpatError

from uuid import uuid4

import rdflib

from rdflib import Literal, URIRef, ConjunctiveGraph as G

if __name__ == "__main__":
  if len(sys.argv) >= 3:
    from_filename, to_filename = sys.argv[1], sys.argv[2]    
    try:
      with open(from_filename, "r") as mdxml_handle:
        mdxml = mdxml_handle.read()
        md_etree = ET.fromstring(mdxml)
    except ExpatError, e:
      print "Expat couldn't parse file: %s" % from_filename
      print e
      sys.exit(2)
    
    data = {}
    for field in [x for x in md_etree.findall("{http://www.xml-cml.org/schema}metadata") if not x.attrib['name'] == "ROBOTS"]:
      name = field.attrib['name']
      if name == "keywords":
        name = "DC.subject"
      if not data.has_key(name):
        data[name] = []
      data[name].append(field.attrib['content'])
    
    if data.has_key("DC.identifier"):
      g_id = URIRef(data["DC.identifier"][0])
    else:
      print "Couldn't find a dc:identifier field in this record %s" % from_filename
      g_id = URIRef("urn:uuid:%s" % uuid4().hex)
      print "Using %s for record %s" % (g_id, from_filename)
    
    if data.has_key("DC.link"):
      record_from = URIRef(data["DC.link"][0])
    else:
      print "Couldn't find a dc:link field in this record %s" % from_filename
      record_from = URIRef("urn:uuid:%s" % uuid4().hex)
      print "Using uuid:%s for record %s" % (g_id, from_filename)
    
    g = G(identifier=record_from)
    nss = {"prism": "http://prismstandard.org/namespaces/1.2/basic/",
           "dc": "http://purl.org/dc/elements/1.1/",
           "dcterms": "http://purl.org/dc/terms/"}
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
    
    if len(sys.argv) == 4:
      print "Output for %s: (in rdf/xml)" % from_filename
      print g.serialize(format="xml")
    
    with open(to_filename, "w") as output:
      output.write(g.serialize(format="xml").encode("utf-8"))

    
