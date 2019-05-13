import base64
import json

file = open("/home/gumbew/workspace/Kursova/swarm-mr-client/../../client_data/reducer.py", 'r')
file_content = file.read()
encoded = base64.b64encode(bytes(file_content,'utf-8'))
decoded = encoded.decode('utf-8')
exec(base64.b64decode(decoded))
print(custom_reducer)
