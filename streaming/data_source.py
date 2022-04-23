import socket
import sys
import requests
import json
import time
import re
import socket
import os
import datetime
#import pyspark

TCP_IP = "0.0.0.0"
TCP_PORT = 9999
conn = None
url = 'https://api.github.com/search/repositories?q=+language:Csharp+language:Python+language:Java&sort=updated&order=desc&per_page=50'

token = os.getenv('TOKEN')
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting Git Info.")

while True:
    try:
        res = requests.get(url, headers={"Authorization": token})
        result_json = res.json()
        for i in range (len(result_json.get("items"))):
            print(result_json.get("items")[i].get("language"), result_json.get("items")[i].get("full_name"), result_json.get("items")[i].get("pushed_at"), result_json.get("items")[i].get("stargazers_count"), result_json.get("items")[i].get("description"), sep='\t')
            data = str(result_json.get("items")[i].get("language")) + "\t" + str(result_json.get("items")[i].get("full_name")) + "\t" + str(result_json.get("items")[i].get("pushed_at")) + "\t" + str(result_json.get("items")[i].get("stargazers_count")) + "\t" + str(result_json.get("items")[i].get("description")) + "\n"
            conn.send(data.encode())
        time.sleep(15)
    except KeyboardInterrupt:
        s.shutdown(socket.SHUT_RD)


