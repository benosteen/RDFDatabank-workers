#!/usr/bin/env python

# TODO - make the provenance appending atomic via redis and shunt the periodic to-disc synchronisation to bkgroud worker task

from ofs.local import OFS

try:
  import json
except ImportError:
  import simplejson as json

from datetime import datetime

PROVENANCE_LABEL = "object_provenance.json"

class POFS(OFS):
  def log_provenance(self, bucket, **prov_kw):  #artifact, process, agent, label, etc
    # check for existance of provenance record
    if not self.exists(bucket, PROVENANCE_LABEL):
      json_payload = {}
    else:
      payload = self.get_stream(bucket, PROVENANCE_LABEL).read()
      json_payload = json.loads(payload)
    if not ( json_payload.has_key('provenance') and isinstance(json_payload.get('provenance'), list) ):
      json_payload['provenance'] = []
    prov_kw["_timestamp"] = datetime.now().isoformat()
    json_payload['provenance'].append(prov_kw)
    super(POFS, self).put_stream(bucket, label = PROVENANCE_LABEL, stream_object = json.dumps(json_payload))
  
  def get_provenance(self):
    if not self.exists(bucket, PROVENANCE_LABEL):
      json_payload = {}
    else:
      payload = self.get_stream(bucket, PROVENANCE_LABEL).read()
      json_payload = json.loads(payload)
    if json_payload:
      return list(json_payload.get('provenance'))   # 'clone'
    else:
      return []
  
  def put_stream(self, bucket, label, stream_object, usedArtifacts, agent, process, additionalOutputs=[], params={}):
    super(POFS, self).put_stream(bucket, label, stream_object, params)
    outputs = [label]
    outputs.extend(additionalOutputs)
    self.log_provenance(bucket, label=label, artifact=usedArtifacts, agent=agent, process=process, outputs=outputs)

  def put_stream_without_provenance(self, bucket, label, stream_object, params={}):
    super(POFS, self).put_stream(bucket, label, stream_object, params)

  def report(self, bucket, form="display"):
    """Generates a report for a given bucket/object
             form = 'display' : print-able format
                      'json'    : JSON-encoded form
                                           """
    if not self.exists(bucket, PROVENANCE_LABEL):
      json_payload = {}
    else:
      payload = self.get_stream(bucket, PROVENANCE_LABEL).read()
      json_payload = json.loads(payload)
    if json_payload:
      if not ( json_payload.has_key('provenance') and isinstance(json_payload.get('provenance'), list) ):
        return "%s has no provenance record" % bucket
      if form.lower() == "display":
        def provenance_record(data):
          record = []
          for key in sorted(data.keys()):
            record.append("%s - %s" % (key, data[key]))
          record.append("-="*20)
          return "\n".join(record)
        report = []
        for item in json_payload.get('provenance'):
          report.append(provenance_record(item))
        return "\n".join(report)
      elif form.lower() == "json":
        return payload
