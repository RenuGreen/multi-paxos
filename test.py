import socket
from thread import *
import threading
import time
import json
import Queue
import traceback, random
import re
import logging


with open("config.json", "r") as configFile:
    config = json.load(configFile)

logging.basicConfig(level = logging.INFO)

leader_id = "1"  #everything is a string (leader_id, process_id, etc.)
send_channels = {}
recv_channels = []
message_queue = Queue.Queue()
heartbeat_message_queue = Queue.Queue()
message_queue_lock = threading.RLock()
heartbeat_queue_lock = threading.RLock()


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
        if not "client" in send_channels: #connection to client
            client_ip = config["client"][process_id]["IP"]
            client_port = config["client"][process_id]["Port"]
            try:
                client_soc = socket.socket()
                client_soc.connect((client_ip, client_port))
                print 'Connected to client ' + client_ip + ' on port ' + str(client_port)
                send_channels["client"] = client_soc
            except:
                pass
        for i in config["dc"].keys():
            if not i == process_id and not i in send_channels.keys():
                cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                host = config["dc"][i]["IP"]
                port = config["dc"][i]["Port"]
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
                if receiver in send_channels.keys():
                    print 'trying to send', message
                    send_channels[receiver].sendall(json.dumps(message))
            except:
                print 'could not send', message
                print traceback.print_exc()


def receive_message():
    while True:
        print 'checking in while ', len(recv_channels)
        for socket in recv_channels:
            try:
                print 'checking in msg'
                message = socket.recv(4096)
                print message
                r = re.split('(\{.*?\})(?= *\{)', message)
                for msg in r:
                    if msg == '\n' or msg == '' or msg is None:
                        continue
                    msg = json.loads(msg)
                    msg_type = msg["message_type"]
                    print msg
                    if msg_type == "HEARTBEAT":
                        heartbeat_message_queue.put(msg)
                    print 'outside check message type'
            except:
                print traceback.print_exc()
                print 'in error'
                #print 'in exception'
                time.sleep(1)
                continue
    print 'outside while'

def broadcast_msg(message):
    try:
        for i in config["dc"]:
            if i != process_id:
                message_copy = dict(message)
                message_copy['receiver_id'] = i
                message_queue_lock.acquire()
                message_queue.put(message_copy)
                message_queue_lock.release()
    except:
        print traceback.print_exc()
            #if message["message_type"] == "COMMIT":
            #    time.sleep(3)

def send_heartbeat():
    c = 0
    while True:
        try:
            if process_id == leader_id:
                msg = {"message_type": "HEARTBEAT", "sender_id": process_id, "c": c}
                broadcast_msg(msg)
                time.sleep(5)
                c += 1
        except:
            #print traceback.print_exc()
            print 'send heartbeat stopped'


def receive_heartbeat():
    time.sleep(8)
    global leader_id
    while True:
        if process_id != leader_id:
            try:
                if heartbeat_message_queue.qsize() > 0:
                    while heartbeat_message_queue.qsize() > 1:
                        heartbeat_message_queue.get()
                    msg = heartbeat_message_queue.get()
                    leader_id = msg["sender_id"]
                    print 'got heartbeat from with ballot and index and count', leader_id,  msg["c"]
                else:
                    print 'no leader'
                    leader_id = None
                time.sleep(7)
            except:
                print 'rcv heartbeat stopped'
                print traceback.print_exc()


################################################################################
if __name__ == "__main__":

    process_id = raw_input("Enter process id: ")

    HOST = config["dc"][process_id]["IP"]
    PORT = config["dc"][process_id]["Port"]
    print PORT
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ## s.setblocking(0)
    s.bind((HOST, PORT))
    s.listen(10)

    start_new_thread(setup_receive_channels, (s,))
    start_new_thread(setup_send_channels, ())
    # t1 = threading.Thread(target=setup_send_channels, args=())
    # t1.start()
    start_new_thread(send_message, ())
    start_new_thread(receive_message, ())
    start_new_thread(send_heartbeat, ())
    start_new_thread(receive_heartbeat, ())

    while True:
        pass



