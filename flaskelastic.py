from flask import Flask, json, request, send_file
from elasticsearch import Elasticsearch
from datetime import datetime
import os
import csv 

api = Flask(__name__)

@api.route('/read', methods=['GET'])
def get_read():
  index = request.args.get('index')
  size = request.args.get('size')
  try:
    elastic_client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    resp_search = elastic_client.search(index=index, query={"match_all": {}}, size=size)
    result = {
      'size': len(resp_search["hits"]["hits"])
    }
    result['data'] = []
    for hit in resp_search['hits']['hits']:
      result['data'].append(hit)
    return json.dumps(result)
  except Exception as err:
    return 'Error index not found'

@api.route('/start_read_all', methods=['GET'])
def get_start_read_all():
  index = request.args.get('index')
  try:
    elastic_client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    resp_search = elastic_client.search(index=index, query={"match_all": {}})
    csv_dir  = './csv/'
    csv_filename = '{timestamp}_{filename}'.format(filename=index, timestamp=datetime.now().strftime("%Y%m%d%H%M%S"))
    csv_file = csv_filename + '.csv'
    csv_path = os.path.join(csv_dir, csv_file)
    csv_column = resp_search['hits']['hits'][0].keys()
    with open(csv_path, 'w+') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_column)
        writer.writeheader()
        for data in resp_search['hits']['hits']:
            writer.writerow(data)
    result = {
      'job_id': csv_filename
    }
    return json.dumps(result)
  except Exception as err:
    return 'Error index not found'

@api.route('/read_all', methods=['GET'])
def get_read_all():
  job_id = request.args.get('job_id')
  try:
    csv_dir  = "./csv"
    csv_file = "{filename}.csv".format(filename=job_id)
    csv_path = os.path.join(csv_dir, csv_file)
    if not os.path.isfile(csv_path):
        return "File not found: file %s was not found on the server" % csv_file
    return send_file(csv_path, as_attachment=True, attachment_filename=csv_file)
  except Exception as err:
    return 'Error job_id not found'

if __name__ == '__main__':
    api.run() 