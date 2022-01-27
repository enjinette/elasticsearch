from flask import Flask, json, request
from elasticsearch import Elasticsearch

companies = [{"id": 1, "name": "Company One"}, {"id": 2, "name": "Company Two"}]

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
    print(err)

@api.route('/start_read_all', methods=['GET'])
def get_start_read_all():
  index = request.args.get('index')
  try:
    elastic_client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    doc = {
      'task': 'This is a search task',
      'description': 'Search data by index and size'
    }
    resp = elastic_client.index(index=index, document=doc)
    result = {
      'job_id': resp['_id']
    }
    return json.dumps(result)
  except Exception as err:
    print(err)

@api.route('/read_all', methods=['GET'])
def get_read_all():
  job_id = request.args.get('job_id')
  try:
    elastic_client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
  except Exception as err:
    print(err)

if __name__ == '__main__':
    api.run() 