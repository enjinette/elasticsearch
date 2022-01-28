from fileinput import filename
from flask import Flask, json, request, send_file
from elasticsearch import Elasticsearch
from datetime import datetime
import os
import csv 

api = Flask(__name__)

@api.route('/read', methods=['GET'])
def get_read():
  # Retrieve parameters from the request
  index = request.args.get('index')
  size = request.args.get('size')
  try:
    # Connect to Elasticsearch
    elastic_client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    # Search from elasticsearch by index and size
    resp_search = elastic_client.search(index=index, query={"match_all": {}}, size=size)
    result = {
      'size': len(resp_search["hits"]["hits"])
    }
    result['data'] = []
    # Append all found data to a list
    for hit in resp_search['hits']['hits']:
      result['data'].append(hit)
    return json.dumps(result)
  except Exception as err:
    return 'Error index not found'

@api.route('/start_read_all', methods=['GET'])
def get_start_read_all():
  # Retrieve parameter from the request
  index = request.args.get('index')
  try:
    # Connect to Elasticsearch
    elastic_client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    # Search from elasticsearch by index
    resp_search = elastic_client.search(index=index, query={"match_all": {}})
    # Set CSV filename based on datetime and given index
    csv_dir  = './csv/'
    csv_filename = '{timestamp}_{filename}'.format(filename=index, timestamp=datetime.now().strftime("%Y%m%d%H%M%S"))
    csv_file = csv_filename + '.csv'
    csv_path = os.path.join(csv_dir, csv_file)
    csv_column = resp_search['hits']['hits'][0].keys()
    # Write data to a csv file
    with open(csv_path, 'w+') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_column)
        writer.writeheader()
        for data in resp_search['hits']['hits']:
            writer.writerow(data)
    # Set response data
    result = {
      'job_id': csv_filename
    }
    return json.dumps(result)
  except Exception as err:
    return 'Error index not found'

@api.route('/read_all', methods=['GET'])
def get_read_all():
  # Retrieve parameter from the request
  job_id = request.args.get('job_id')
  try:
    # Set CSV filename based on job_id parameter
    csv_dir  = './csv'
    csv_file = '{filename}.csv'.format(filename=job_id)
    csv_path = os.path.join(csv_dir, csv_file)
    if not os.path.isfile(csv_path):
        return 'File not found: file {filename} was not found on the server'.format(filename=csv_file)
    return send_file(csv_path, as_attachment=True, attachment_filename=csv_file)
  except Exception as err:
    return 'Error job_id not found'

if __name__ == '__main__':
    api.run() 