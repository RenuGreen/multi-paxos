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

class Acceptor:
    log = {}
    leader_id = 1
    manage_log = {}
    manage_ballot = {}

    def receive_accept(self, message):

        log_index = message['log_index']
        if log_index not in Acceptor.manage_log:
            Acceptor.manage_log[log_index] = {'accept_num': 0, 'accept_val' : 0}
        if log_index not in Acceptor.manage_ballot:
            Acceptor.manage_ballot[log_index] = {'ballot_number': 0}
        if message['ballot_number'] >= Acceptor.manage_ballot[log_index]['ballot_num']:
            Acceptor.manage_log[log_index]['accept_num'] = message['ballot_number']
            Acceptor.manage_log[log_index]['accept_val'] = message['value']
            Acceptor.manage_ballot[log_index]['ballot_num'] = message['ballot_number']
            message = {"message_type": "ACCEPT-ACCEPT", "ballot_number" : message['ballot_number'], "value" : message['value'], "receiver_id" : Acceptor.leader_id, "sender_id": process_id}
            send_message(message)


    def receive_final_value(self):

        Acceptor.log[message['log_index']] = message["value"]




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
