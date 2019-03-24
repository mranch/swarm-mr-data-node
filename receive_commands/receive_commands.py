'''
File where data node receives requests from management node about:
1) file(segment) creation
2) map() method start
3) shuffle() method start (where necessary)
4) reduce() method start
5) request to return a status(if works) and file size
'''
# TODO: define methods which implement each request separately
import os


def make_file(path):
    dir_name = path.split('\\')[-1]
    if not os.path.isdir('data\\' + dir_name):
        os.makedirs('data\\' + dir_name)


def write(content):
    pass


def map(field_delimiter, key, source, dest):
    content = open(source).readlines()
    key_list = key.split(',')
    res = list()
    for field in content:
        line = field.split(field_delimiter)
        res_line = str()
        for item in key_list:
            res_line += line[int(item)]
            res_line += field_delimiter
        res_line = res_line[:-1] + '^'
        for item in field:
            res_line += item
        res.append(res_line)
    open(dest, 'w+').writelines(res)
    return res


map('|', '0', "C:\\Users\\smart\\workspace\\client_data\\text.txt",
    "C:\\Users\\smart\\workspace\\client_data\\test_out.txt")
