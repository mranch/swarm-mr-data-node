'''
File where shuffle() method is implemented
'''
import json
# TODO: define shuffle(method) that sorts keys between data nodes after map() method in order to proceed with reduce() method
import os

from receive_commands.receive_commands import hash_f


def shuffle(content):
    print("SHUFFLE")
    print(os.getcwd())
    dir_name = content['file_name'].split(os.sep)[-1]
    path = os.path.join(os.path.dirname(__file__), '..', 'data', dir_name)
    arbiter_node_json_data = open(os.path.join('config', 'data_node_info.json'))

    arbiter_node_data = json.load(arbiter_node_json_data)['self_address']
    print(path)
    files = []

    list_of_lines = list()

    result = {
        'shuffle_items': [

        ]
    }
    for i in content['nodes_keys']:
        result['shuffle_items'].append({'data_node_ip': i['data_node_ip'], 'content': []})
    # r=root, d=directories, f = files

    for r, d, f in os.walk(path):
        for file in f:
            files.append(os.path.join(r, file))
    for f in files:
        for line in open(f):

            for item in content['nodes_keys']:
                # print(item['hash_keys_range'][1])
                # print(item['hash_keys_range'][1] == content['max_hash'])
                if item['hash_keys_range'][1] == content['max_hash']:
                    if item['hash_keys_range'][0] <= hash_f(line.split('^')[0]) and hash_f(line.split('^')[0]) <= \
                            item['hash_keys_range'][1]:

                        for i in result['shuffle_items']:
                            if i['data_node_ip'] == item['data_node_ip']:
                                i['content'].append(line)

                else:
                    if item['hash_keys_range'][0] <= hash_f(line.split('^')[0]) and hash_f(line.split('^')[0]) < \
                            item['hash_keys_range'][1]:
                        for i in result['shuffle_items']:
                            if i['data_node_ip'] == item['data_node_ip']:
                                i['content'].append(line)

    print(result)

    print(len(content['nodes_keys']))

# shuffle({'nodes_keys': [{'data_node_ip': '127.0.0.1:8014', 'hash_keys_range': [612, 618.5]}, {'data_node_ip': '127.0.0.1:8015', 'hash_keys_range': [618.5, 625.0]}], 'max_hash': 625, 'file_name':'.\\..\\client_data\\out.txt'})
