import socket
import json
from thread import *
import threading
import re, time

request_queue = {}
request_queue_lock = threading.Lock()
send_socket_lock = threading.Lock()
REQUEST_TIMEOUT = 15

def send_buy_request(message):
    while True:
        message_id = message["message_id"][0]
        request_queue_lock.acquire()
        request_queue[message_id] = formatted_msg
        request_queue_lock.release()

        send_socket_lock.acquire()
        send_socket.sendall(json.dumps(message))
        send_socket_lock.release()

        time.sleep(REQUEST_TIMEOUT)
        request_queue_lock.acquire()
        print message_id
        if message_id not in request_queue:
             request_queue_lock.release()
             break
        request_queue_lock.release()
        print "retry req"
    return



def handle_show_reply(msg):
    available_tickets = msg["tickets_available"]
    log = msg["log"]
    print "Tickets available: ", available_tickets
    print "Log: ", log

def receive_message_from_dc(s):
    while True:
        try:
            message = s.recv(4096)
            r = re.split('(\{.*?\})(?= *\{)', message)
            for msg in r:
                if msg == '\n' or msg == '' or msg is None:
                    continue
                msg = json.loads(msg)
                msg["message_id"] = tuple(msg["message_id"])
                msg_type = msg["message_type"]
                if msg_type == "SHOW":
                    handle_show_reply(msg)
                if msg_type == "SALE":
                    print msg
                    request_queue_lock.acquire()
                    if msg["message_id"][0] in request_queue:
                        print "removing from req queue"
                        del request_queue[msg["message_id"][0]]
                    request_queue_lock.release()
        except:
            time.sleep(1)
            continue


with open("config.json", "r") as configFile:
    config = json.load(configFile)

message_id = 1
index = raw_input("enter datacenter id")

client_IP = config["client"][index]["IP"]
client_port = config["client"][index]["Port"]
receive_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
receive_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
receive_socket.setblocking(0)
receive_socket.bind((client_IP, client_port))
receive_socket.listen(10)

while True:
    try:
        conn, addr = receive_socket.accept()
        print "connection from dc"
        break
    except:
        continue

start_new_thread(receive_message_from_dc, (conn,))


send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
dc_IP = config["dc"][index]["IP"]
dc_port = config["dc"][index]["Port"]
try:
    send_socket.connect((dc_IP, dc_port))
    print "connected to datacenter ", dc_IP, ":", dc_port
except:
    print "couldn't connect"


while True:
    client_input = raw_input("Enter BUY:<no_of_tickets> or SHOW")
    formatted_msg = {}
    if "BUY" in client_input:
        number_of_tickets = client_input.split(":")[1]
        message_id += 1
        if int(number_of_tickets) > 0:
            number_of_tickets = int(number_of_tickets)
            formatted_msg = {"message_type": "BUY", "number_of_tickets": number_of_tickets, "message_id": (message_id, index)}
            start_new_thread(send_buy_request, (formatted_msg,))

    elif "SHOW" in client_input:
        formatted_msg = {"message_type": "SHOW", "message_id": (message_id, index)}
        send_socket_lock.acquire()
        send_socket.sendall(json.dumps(formatted_msg))
        send_socket_lock.release()
