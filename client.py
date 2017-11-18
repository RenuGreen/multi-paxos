import socket
from thread import *
import threading
import time
import json
import Queue
import traceback, random
import re
import logging
import math

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

    def receive_prepare(self, message):

        log_index = message['log_index']
        if log_index not in Acceptor.manage_ballot:
            Acceptor.manage_ballot[log_index] = {'ballot_number': 0}
        if log_index not in Acceptor.manage_log:
            Acceptor.manage_log[log_index] = {'accept_num': 0, 'accept_val' : 0}
        if message['ballot_number'] >= Acceptor.manage_ballot[log_index]['ballot_num']:
            Acceptor.manage_ballot[log_index] = {'ballot_number': message['ballot_number']}
            message = {"message_type": "ACCEPT-PREPARE", "ballot_number": message['ballot_number'], "accept_num": Acceptor.manage_log[log_index]['accept_num'], "accept_val" : Acceptor.manage_log[log_index]['accept_val'], "receiver_id": Acceptor.leader_id, "sender_id": process_id}
            send_message(message)


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
                    print msg
                    msg_type = msg["message_type"]
                    if(msg_type == "accept-accept"):
                        #receive ack
                        pass


            except:
                time.sleep(1)
                continue

def broadcast_msg(msg):
    for i in config:
        if i != process_id:
            message_copy = dict(message)
            message_copy['receiver_id'] = str(i)
            message_queue_lock.acquire()
            message_queue.put(message_copy)
            message_queue_lock.release()

class Proposer:

    ## Phase I
    # Proposer -> prepare
    # Acceptor -> accept-prepare
    ## Phase II
    # Proposer -> accept
    # Acceptor -> accept-accept
    # Proposer -> commit

    ballot_number = 0
    unchosen_index = 0
    log = {}    # { log_index : value }
    majority = math.ceil(len(config)/2.0)
    log_status = {}     ## { 0 : "ballot_number":n, "value":val, "prepare_count":n, "accept_count":n }


    #NOTE: unchosen index will have to be searched for in the log in case of leader failures

    def send_prepare(self):
        msg = {"message_type": "PREPARE", "ballot_number": (Proposer.ballot_number, process_id),
               "log_index": Proposer.unchosen_index, "value": value, "sender_id": process_id}

    def receive_accept_prepare(self, msg):
        log_index = msg["log_index"]
        accept_num = msg["accept_num"]
        accept_val = msg["accept_val"]
        if log_index in Proposer.ballot_status:
            old_accept_num = Proposer.ballot_status[log_index]["accept_num"]
            if accept_num > old_accept_num:
                Proposer.ballot_status[log_index]["accept_num"] = accept_num
                Proposer.ballot_status[log_index]["accept_val"] = accept_val
        else:
            Proposer.ballot_status[log_index] = { "accept_num": accept_num, "accept_val":accept_val }

        if "prepare_count" in Proposer.log_status[log_index]:
            Proposer.log_status[log_index]["prepare_count"] = 1
        else:
            Proposer.log_status[log_index]["prepare_count"] += 1
        self.check_prepare_status(log_index)

    def check_preare_status(self, log_index):
        if Proposer.log_status[log_index]["accept_count"] >= Proposer.majority:
            value = Proposer.ballot_status[log_index]["accept_val"]
            Proposer.log_status[log_index]["value"] = value
            self.send_accept_msg(value)

    def send_accept_msg(self, value):
        msg = { "message_type" : "ACCEPT", "ballot_number" : (Proposer.ballot_number, process_id), "log_index" : Proposer.unchosen_index, "value" : value, "sender_id" : process_id }
        Proposer.log_status[Proposer.unchosen_index] = { "ballot_number" : Proposer.ballot_number, "value" : value }
        Proposer.ballot_number += 1
        Proposer.unchosen_index += 1
        broadcast_msg(msg)

    def receive_ack(self, msg):
        log_index = msg["log_index"]
        if Proposer.log_status[log_index]["accept_count"]:
            Proposer.log_status[log_index]["accept_count"] = 1
        else:
            Proposer.log_status[log_index]["accept_count"] += 1
        self.check_log_status(log_index)

    def check_log_status(self, log_index):
        if Proposer.log_status[log_index]["accept_count"] >= Proposer.majority:
            self.send_final_accept(log_index)

    def send_final_accept(self, log_index):
        ballot_number = Proposer.log_status[log_index]["ballot_number"]
        value = Proposer.log_status[log_index]["value"]
        Proposer.log[log_index] = value
        msg = { "message_type": "COMMIT", "ballot_number": (ballot_number, process_id), "log_index": log_index, "value": value, "sender_id": process_id }
        broadcast_msg(msg)







################################################################################


with open("config.json", "r") as configFile:
    config = json.load(configFile)

process_id = raw_input("Enter process id: ")

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
# t1.join()
start_new_thread(send_message, ())
start_new_thread(receive_message, ())




while True:
    message = raw_input("Enter SNAPSHOT: ")
    if message == "SNAPSHOT":
        print ''
