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
    if not os.path.isdir('data\\'+dir_name):
        os.makedirs('data\\'+dir_name)

def write(content):
    pass
