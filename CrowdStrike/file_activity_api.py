from dateutil.parser import parse
from datetime import timedelta
from flask import Flask
from flask_restplus import Resource, Api
from flask_restplus import fields, marshal
from redisgraph import Node, Edge, Graph, Path
import redis
import json

r = redis.Redis(host='localhost', port=6379)
redis_graph = Graph('file_activity1', r)

app = Flask(__name__)
api = Api(app)
red = redis.Redis()

_model1 = api.model("Meta", {'_id': fields.Integer,
  'date_added': fields.String,
  'name': fields.String,
  'platform': fields.String,
  'downloaded': fields.List(fields.List(fields.String)),
  'executed': fields.List(fields.List(fields.String)),
  'removed': fields.List(fields.List(fields.String))})

# CRUD Operations

@api.route('/addorupdate')
class AddMeta(Resource):
    @api.expect(_model1)
    def post(self):
        """Add or Update File Meta Data and Changes Relations Accordingly"""
        print(api.payload)
        _node = Node(label='file', properties={'fid':api.payload['_id'], 'name':api.payload['name'], 'date_added':api.payload['date_added'], 'platform':api.payload['platform']})
        redis_graph.add_node(_node)
        redis_graph.commit()
        
        for fileid, time_stamp in api.payload['downloaded']:
            edge = Edge(api.payload['_id'] , 'DOWNLOADED', fileid, properties={'time': time_stamp, 'activity':'downloaded'})
            redis_graph.add_edge(edge)

        for fileid, time_stamp in api.payload['executed']:
            edge = Edge(api.payload['_id'] , 'EXECUTED', fileid, properties={'time': time_stamp, 'activity':'executed'})
            redis_graph.add_edge(edge)

        for fileid, time_stamp in api.payload['removed']:
            edge = Edge(api.payload['_id'] , 'REMOVED', fileid, properties={'time': time_stamp, 'activity':'removed'})
            redis_graph.add_edge(edge)

        return {'status': 'OK'}

@api.route('/view/<file>')
class ViewMeta(Resource):
    def get(self, file):
        """Finds the Meta Data of the file and all its relations"""
        params, results, stats = r.execute_command('GRAPH.QUERY', 'file_activity1', f"MATCH (f1:file {{name:{file}}})-[r:*]-(f2:file) RETURN f1.name, r.name, r.time, f2.name")
        return {"status": 'OK'}

@api.route('/del/<file>')
class DelMeta(Resource):
    def delete(self, file):
        """Deletes the file and all its relations"""
        params, results, stats = r.execute_command('GRAPH.QUERY', 'file_activity1', f"MATCH MATCH (f:file {{name:{file}}})-[*]-(:file) DETACH DELETE f")
        return {"status": 'OK'}

# Queries

def get_activities_for(file, act):
    """Utility function"""
    params, results, stats = r.execute_command('GRAPH.QUERY', 'file_activity1', f"MATCH(f1:file)-[r:{act}]->(f2:file) WHERE f1.name='{file}' RETURN r.time,  f2.fid, f2.name, f2.date_added, f2.platform")
    data = []
    for _time, _fid, _file, _created, _platform  in results:
        d = {'time': _time.decode(),
            'id': _fid,
            'file_name': _file.decode(),
            'time_created': _created.decode(),
            'platform': _platform.decode()}

        data.append(d)
    return data

@api.route('/listall/<file>')
class AllActivity(Resource):
    def get(self, file):
        "List Out files metadata and files it downloaded, executed, removed and time of activity"
        activities = []
        activities.append(get_activities_for(file, 'DOWNLOADED'))
        activities.append(get_activities_for(file, 'EXECUTED'))
        activities.append(get_activities_for(file, 'REMOVED'))
        resource_fields = {
            'time': fields.String,
            'id': fields.Integer,
            'file_name': fields.String,
            'time_created': fields.String,
            'platform': fields.String}
        return marshal(activities, resource_fields)

_model2 = api.model("Query", {
  'platform': fields.String,
  'type': fields.String,
  'date': fields.String,
  'limit': fields.Integer})

@api.route('/query')
class ListByParam(Resource):
    @api.expect(_model2)
    def get(self):
        """Returns list of all files for the given type, which performed activity in the given time window"""
        _date1 = parse(api.payload['date'])
        _date2 =_date1 + timedelta(hours=api.payload['limit'])
        params, results, stats = r.execute_command('GRAPH.QUERY', 'file_activity1', f"MATCH (f1:file{{name:{api.payload['platform']}}})-[act:{type}]->(f2:file) WHERE act.time > {_data1.isoformat()} AND act.time < {_data2.isoformat()} RETURN f1, act.time")
        resource_fields =  { 'file_name': fields.String, 'time_of_activity':fields.String}
        return marshal(results, resource_fields)

_model3 = api.model("Path", {
  'src_name': fields.String,
  'dst_name': fields.String,
  'platform': fields.String})

@api.route('/path')
class ListFileRelations(Resource):
    @api.expect(_model3)
    def get(self, src, dst):
        """Returns the first (Shortest) Path between two files, cronologically"""
        resource_fields =  {
        'src_name': fields.String,
        'src_date_added': fields.String,
        'dst_name': fields.String,
        'dst_date_added': fields.String,
        'activity': fields.String,
        'activity_date':fields.String
        }
        params, results, stats = r.execute_command('GRAPH.QUERY', 'file_activity1', f"MATCH p=shortestPath(f1:file{{name:{api.payload['src_name']}}})-[*]->(f2:file{{name:{api.payload['dst_name']}}}) WHERE f1.date_added < f2.date_added RETURN p")
        return marshal(results, resource_fields)

if __name__ == '__main__':
    app.run(debug=True)