import socket
from thread import *
import threading
import time
import json
import Queue
import traceback, random
import re
import logging

logging.basicConfig(level = logging.INFO)

send_channels = {}
recv_channels = []
message_queue = Queue.Queue()
lock=threading.Lock()
message_queue_lock = threading.Lock()

def setup_receive_channels(s): 
    while 1:
        try:
            conn, addr = s.accept()
        except:
            continue
        recv_channels.append(conn)
        print 'Connected with ' + addr[0] + ':' + str(addr[1])
        # what to do after connecting to all clients
        # should I break?

def setup_send_channels():
    while True:
        for i in config.keys():
            if not i == process_id and not i in send_channels.keys():
                cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                host = '127.0.0.1'
                port = config[i]
                try:
                    cs.connect((host, port))
                except:
                    time.sleep(1)
                    continue
                print 'Connected to ' + host + ' on port ' + str(port)
                send_channels[i] = cs   #add channel to dictionary

def send_message():
    while True:
        if message_queue.qsize() > 0:
            try:
                message_queue_lock.acquire()
                message = message_queue.get()
                message_queue_lock.release()
                receiver = message["receiver_id"]
                send_channels[receiver].sendall(json.dumps(message))
            except:
                print traceback.print_exc()


def receive_message():
    while True:
        for socket in recv_channels:
            try:
                message = socket.recv(4096)
                r = re.split('(\{.*?\})(?= *\{)', message)
                for msg in r:
                    if msg == '\n' or msg == '' or msg is None:
                        continue
                    msg = json.loads(msg)
                    msg_type = msg["message_type"]



            except:
                time.sleep(1)
                continue


################################################################################


with open("config.json", "r") as configFile:
    config = json.load(configFile)

process_id = raw_input()

HOST = ''
PORT = config[process_id]
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.setblocking(0)
s.bind((HOST, PORT))
s.listen(10)

start_new_thread(setup_receive_channels, (s,))
t1 = threading.Thread(target=setup_send_channels, args=())
t1.start()
# wait till all send connections have been set up
t1.join()
start_new_thread(send_message, ())
start_new_thread(receive_message, ())


while True:
    message = raw_input("Enter SNAPSHOT: ")
    if message == "SNAPSHOT":
        print ''
