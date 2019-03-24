from http import server
import json
from multiprocessing import Process
from receive_commands import receive_commands as rc
import os

class Handler(server.BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(200)
        self.end_headers()
        body_length = int(self.headers['content-length'])
        request_body_json_string = self.rfile.read(body_length).decode('utf-8')

        # Printing  some info to the server console
        print('Server on port ' + str(self.server.server_port) + ' - request body: ' + request_body_json_string)

        json_data_obj = json.loads(request_body_json_string)
        json_data_obj['SEEN_BY_THE_SERVER'] = 'Yes'

        print(request_body_json_string)
        #rc.make_file(json_data_obj["file_name"])

        self.wfile.write(bytes(json.dumps(self.recognize_command(json_data_obj)), 'utf-8'))


    def recognize_command(self,content):
        json_data_obj = dict()
        print(content)
        if 'make_file' in content:
            json_data_obj = content['make_file']
            print(rc.make_file(json_data_obj["file_name"]))
        elif 'write' in content:
            json_data_obj = content['write']
            print(rc.write(json_data_obj))
        return json_data_obj

def start_server(server_address):
    my_server = server.ThreadingHTTPServer(server_address, Handler)
    print(str(server_address) + ' Waiting for POST requests...')
    my_server.serve_forever()


def start_local_server_on_port(port):
    p = Process(target=start_server, args=(('127.0.0.1', port),))
    p.start()


if (__name__ == '__main__'):
    start_local_server_on_port(8014)
    #start_local_server_on_port(8015)