'''
File where shuffle() method is implemented
'''
import json
# TODO: define shuffle(method) that sorts keys between data nodes after map() method in order to proceed with reduce() method
import os
import requests

from receive_commands.receive_commands import hash_f, make_file


class ShuffleCommand:
	def __init__(self, data, file_path):
		self._data = dict()
		self._data['data_node_ip'] = data['data_node_ip']
		self._data['content'] = data['content']
		self._data['file_path'] = file_path

	def send(self):
		data = dict()
		data['finish_shuffle'] = dict()
		data['finish_shuffle']['content'] = self._data['content']
		data['finish_shuffle']['file_path'] = self._data['file_path']

		response = requests.post \
			('http://' + self._data['data_node_ip'],
			 data=json.dumps(data))
		response.raise_for_status()
		print(response.json())
		return response.json()


def shuffle(content):
	dir_name = content['file_name'].split(os.sep)[-1]
	path = os.path.join(os.path.dirname(__file__), '..', 'data', dir_name)
	arbiter_node_json_data = open(os.path.join('config', 'data_node_info.json'))
	self_node_ip = json.load(arbiter_node_json_data)['self_address']
	files = []
	list_of_lines = list()
	result = {
		'shuffle_items': [
		]
	}
	new_dir_name = dir_name.split(os.sep)[-1].split('.')[0].split('_')[0] + '_shuffle' + '.' + \
				   dir_name.split(os.sep)[-1].split('.')[-1]

	if not os.path.isfile(new_dir_name):
		make_file(new_dir_name)

	for i in content['nodes_keys']:
		result['shuffle_items'].append({'data_node_ip': i['data_node_ip'], 'content': []})
	# r=root, d=directories, f = files

	for r, d, f in os.walk(path):
		for file in f:
			files.append(os.path.join(r, file))
	for f in files:
		for line in open(f):

			for item in content['nodes_keys']:
				if item['hash_keys_range'][1] == content['max_hash']:
					if item['hash_keys_range'][0] <= hash_f(line.split('^')[0]) <= item['hash_keys_range'][1]:
						for i in result['shuffle_items']:
							if i['data_node_ip'] == item['data_node_ip']:
								i['content'].append(line)

				else:
					if item['hash_keys_range'][0] <= hash_f(line.split('^')[0]) < item['hash_keys_range'][1]:
						for i in result['shuffle_items']:
							if i['data_node_ip'] == item['data_node_ip']:
								i['content'].append(line)
	for i in result['shuffle_items']:
		if i['data_node_ip'] == self_node_ip:
			f = open(os.path.join(os.path.dirname(__file__), '..', 'data', new_dir_name, 'shuffled'), 'a+')
			f.writelines(i['content'])
		else:
			sc = ShuffleCommand(i, new_dir_name)
			sc.send()
