"""
File where data node receives requests from management node about:
1) file(segment) creation
2) map() method start
3) shuffle() method start (where necessary)
4) reduce() method start
5) request to return a status(if works) and file size
"""
# # TODO: define methods which implement each request separately
import os
import json
import requests


def hash_f(str):
    res = 545
    for i in str:
        res += ord(i)
    return res


def make_file(path):
    dir_name = path.split(os.sep)[-1]
    if not os.path.isdir(os.path.join('data', dir_name)):
        os.makedirs(os.path.join('data', dir_name))


def write(content):
    dir_name = content['file_name'].split(os.sep)[-2]

    file_name = content['file_name'].split(os.sep)[-1]
    path = os.path.join(os.path.dirname(__file__), '..', 'data', dir_name, file_name)

    # if os.path.isdir('\\data\\' + dir_name):
    f = open(path, 'w+')
    f.writelines(content['segment'])
    f.close()


def reduce(reducer, content):
    import collections
    result_dict = collections.Counter()
    dir_name = os.path.join(os.path.dirname(__file__), '..', 'data', content.split(os.sep)[-1])
    for file in os.listdir(dir_name):
        content = open(os.path.join(dir_name, file)).readlines()
        for field in content:
            field = field[:-1]
            for item in content:
                sum = 0
                for i in item.split('^')[1]:
                    sum += ord(i)
                result_dict[item.split('^')[0]] += sum
    result = list()
    for item in result_dict.keys():
        result.append(item + '^' + str(result_dict[item]))

    return result


def map(mapper, field_delimiter, key, dest):
    dir_name = os.path.join('data', dest.split(os.sep)[-1])
    new_dir_name = dir_name.split('\\')[-1].split('.')[0] \
                   + '_map' + '.' + dir_name.split('\\')[-1].split('.')[-1]
    make_file(new_dir_name)
    for file in os.listdir(dir_name):
        content = open(os.path.join(dir_name, file)).readlines()
        key_list = key.split(',')
        res = list()
        for field in content:
            field = field[:-1]
            line = field.split(field_delimiter)
            res_line = str()
            for item in key_list:
                res_line += line[int(item)]
                res_line += field_delimiter
            res_line = res_line[:-1] + '^'
            for item in field:
                res_line += item
            res.append(res_line + '\n')
        f = open(os.path.join(os.path.dirname(__file__), '..', 'data', new_dir_name, file), 'w+')
        f.writelines(res)


def hash_keys(content):
    dir_name = content.split(os.sep)[-1]
    path = os.path.join(os.path.dirname(__file__), '..', 'data', dir_name)
    print(path)
    files = []
    hash_key_list = list()
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            files.append(os.path.join(r, file))
    for f in files:
        for line in open(f):
            hash_key_list.append(hash_f(line.split('^')[0]))
    print(hash_key_list)
    return hash_key_list


def min_max_hash(hash_key_list, file_name):
    arbiter_node_json_data = open(os.path.join('config', 'data_node_info.json'))
    arbiter_node_data = json.load(arbiter_node_json_data)['arbiter_address']
    print(arbiter_node_data)
    res = list()
    res.append(max(hash_key_list))
    res.append(min(hash_key_list))
    url = 'http://' + arbiter_node_data
    diction = {
        'hash':
            {
                'list_keys': res,
                'file_name': file_name
            }
    }
    response = requests.post(url, data=json.dumps(diction))
    return response.json()


def finish_shuffle(content):
    print("FINISH_SHUFFLE_START")
    data = content['finish_shuffle']
    dir_name = data['file_path']
    f = open(os.path.join(os.path.dirname(__file__), '..', 'data', dir_name, 'shuffled'), 'a+')
    f.writelines(data['content'])
    print("FINISH_SHUFFLE_END")
