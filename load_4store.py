from HTTP4Store import HTTP4Store
h = HTTP4Store("http://localhost:8000")

from pofs import POFS
p = POFS("iucr")
def add_all():
  for item in p.list_buckets():
    if p.exists(item, "intermediary.rdf.xml"):
      f = p.get_stream(item, "intermediary.rdf.xml", as_stream=False)
      print "Adding graph for %s to 4Store" % item
      h.add_graph(item, f, content_type="xml")
      print "Success"
    else:
      print "Item %s does not have an 'intermediary.rdf.xml'" % item
