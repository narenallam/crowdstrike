import random
import string
from pprint import pprint
import random
import datetime
from dateutil.parser import parse

# yourdate = dateutil.parser.parse('2020-07-11T04:03:40.252317')

def random_datetime(start=datetime.datetime(2020, 1, 1), 
                    end=datetime.datetime.now()):
    
    delta = end - start
    new_datetime = start + delta * random.random()
    return new_datetime.isoformat()
def activity_gen(idx, rec, data, act):
    activities = []

    for file_data in random.sample(data[idx+1:], random.randint(2, 5)):
        if file_data['name'] != rec['name'] and file_data['platform'] == rec['platform']:
            file_id = file_data['_id']
            if act == 'downloaded':
                time_stamp = file_data['date_added']
            else:
                time_stamp = random_datetime(start=parse(file_data['date_added']))
            activities.append((file_id, time_stamp))
    return activities
        
def data_gen(n):
    a = ["executor", "downloader", "remover"]
    b = ["Windows", "Mac", "Linux"]
    files = string.ascii_uppercase

    data = []

    _ids = set()
    _file_plat = set()
    _files = set()

    for i in range(n+5):
        _id = random.randint(10000, 99999)

        while _id in _ids:
            _id = random.randint(10000, 99999)
        _ids.add(_id)

        cur_file = random.choice(files)
        platform = random.choice(b)

        while (cur_file, platform) in _file_plat:
            cur_file = random.choice(files)
            platform = random.choice(b)

        _file_plat.add((cur_file, platform))
        _files.add(cur_file)

        date_added = random_datetime()


        d = {"_id": _id, 
             "name": cur_file, 
             "date_added": random_datetime(), 
             "platform": random.choice(b),
             'downloaded':[],
             'executed':[],
             'removed':[]
            }
        data.append(d)

    # len(_file_plat) == len(data)
    # sorting data based on date_created
    sorted_data = sorted(data, key=lambda x:x['date_added'])

    for idx, rec in enumerate(sorted_data[:-5]):
        rec['downloaded'] = activity_gen(idx, rec, sorted_data, 'downloaded')
        rec['executed'] = activity_gen(idx, rec, sorted_data, 'executed')
        rec['removed'] = activity_gen(idx, rec, sorted_data, 'removed')

    return sorted_data
if __name__ == '__main__':
    data = data_gen(10)
    pprint(data)

