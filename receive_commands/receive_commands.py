'''
File where data node receives requests from management node about:
1) file(segment) creation
2) map() method start
3) shuffle() method start (where necessary)
4) reduce() method start
5) request to return a status(if works) and file size
'''
# # TODO: define methods which implement each request separately
import os
import json
import requests

def hash_f(str):
    res = 545
    for i in str:
        res+=ord(i)
    return res

def make_file(path):
    dir_name = path.split('\\')[-1]
    if not os.path.isdir('data\\' + dir_name):
        os.makedirs('data\\' + dir_name)


def write(content):
    dir_name = content['file_name'].split('\\')[-2]
    file_name = content['file_name'].split('\\')[-1]
    path = os.path.dirname(__file__) + '\\..\\data\\' + dir_name + '\\' + file_name

    # if os.path.isdir('\\data\\' + dir_name):
    f = open(path, 'w+')
    f.writelines(content['segment'])
    f.close()


def map(mapper, field_delimiter, key, dest):
    LOC = """
def map(field_delimiter, key, dest):
    aa = dest.split('\\\\')[-1]
    dir_name = "data\\\\"+ aa
    for file in os.listdir(dir_name):
        content = open(dir_name+'\\\\'+ file).readlines()
        key_list = key.split(',')
        res = list()
        for field in content:
            field=field[:-1]
            line = field.split(field_delimiter)
            res_line = str()
            for item in key_list:
                res_line += line[int(item)]
                res_line += field_delimiter
            res_line = res_line[:-1] + '^'
            for item in field:
                res_line += item
            res.append(res_line+'\\n')
        open(dir_name+'\\\\'+ file, 'w+').writelines(res)
map(field_delimiter, key, dest)
"""
    exec(LOC)


def hash_keys(content):
    dir_name = content.split('\\')[-1]
    path = os.path.dirname(__file__) + '\\..\\data\\'+ dir_name
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

def min_max_hash(hash_key_list,file_name):
    arbiter_node_json_data = open('config\\data_node_info.json')
    arbiter_node_data = json.load(arbiter_node_json_data)['arbiter_address']
    print(arbiter_node_data)
    res= list()
    res.append(max(hash_key_list))
    res.append(min(hash_key_list))
    url = 'http://' + arbiter_node_data
    diction ={
        'hash':
            {
                'list_keys':res,
                'file_name':file_name
            }
    }
    response = requests.post(url, data=json.dumps(diction))
    return response.json()
    #return res

# Python program to illustrate use of exec to
# execute a given code as string.


# map('|', '0', "C:\\Users\\smart\\workspace\\client_data\\text.txt",
#     "C:\\Users\\smart\\workspace\\client_data\\test_out.txt")

# write({"file_name": 'C:/Users/smart/workspace/KURSOVA/swarm-mr-client\\..\\..\\client_data\\out.txt\\f2', "segment": ['A|1', 'B|2']})

# f = open(os.path.join(os.pardir + '\\data\\', "filename"), "w")
# f.write('hhhhh')
# f.close()
