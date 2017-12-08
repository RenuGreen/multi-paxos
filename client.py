import socket
from thread import *
import threading
import time
import json
import Queue
import traceback, random
import re
import logging
import tickets

with open("config.json", "r") as configFile:
    config = json.load(configFile)

logging.basicConfig(level = logging.INFO)

leader_id = "1"  #everything is a string (leader_id, process_id, etc.)
send_channels = {}
recv_channels = []
message_queue = Queue.Queue()
heartbeat_message_queue = Queue.Queue()
lock = threading.Lock()
message_queue_lock = threading.Lock()
request_queue = []
request_queue_lock = threading.Lock()
message_id = 0
heartbeat_queue_lock = threading.Lock()
log = {}
highest_ballot_received = 0


class Acceptor:

    manage_log = {}
    manage_ballot = {}

    def receive_prepare(self, message):
        log_index = message['log_index']
        if log_index not in Acceptor.manage_ballot:
            Acceptor.manage_ballot[log_index] = {'ballot_number': 0}
        if log_index not in Acceptor.manage_log:
            Acceptor.manage_log[log_index] = {'accept_num': 0, 'accept_val': 0, 'message_id': (0, 0)}
        if message['ballot_number'] > Acceptor.manage_ballot[log_index]['ballot_number']:
            Acceptor.manage_ballot[log_index] = {'ballot_number': message['ballot_number']}
            message = {"message_type": "ACCEPT-PREPARE", "ballot_number": message['ballot_number'], "accept_num": Acceptor.manage_log[log_index]['accept_num'],
                       "accept_val": Acceptor.manage_log[log_index]['accept_val'], "receiver_id": message["sender_id"], "sender_id": process_id, "log_index": log_index, "message_id": Acceptor.manage_log[log_index]['message_id']}
            message_queue.put(message)

    def receive_accept(self, message):
        try:
            log_index = message['log_index']
            if log_index not in Acceptor.manage_log:
                Acceptor.manage_log[log_index] = {'accept_num': 0, 'accept_val': 0}
            if log_index not in Acceptor.manage_ballot:
                Acceptor.manage_ballot[log_index] = {'ballot_number': 0}
            if message['ballot_number'] >= Acceptor.manage_ballot[log_index]['ballot_number']:
                Acceptor.manage_log[log_index]['accept_num'] = message['ballot_number']
                Acceptor.manage_log[log_index]['accept_val'] = message['value']
                Acceptor.manage_log[log_index]['message_id'] = message['message_id']
                Acceptor.manage_ballot[log_index]['ballot_number'] = message['ballot_number']
                message = {"message_type": "ACCEPT-ACCEPT", "ballot_number" : message['ballot_number'], "value" : message['value'],
                           "receiver_id": message["sender_id"], "sender_id": process_id, "log_index": log_index, "message_id": message['message_id']}
                message_queue_lock.acquire()
                message_queue.put(message)
                message_queue_lock.release()
        except:
            print traceback.print_exc()

    def receive_final_value(self, message):
        if message['log_index'] in log:
            print 'printing value already in log ', log
            self.notify_client(message)
            return
        log[message['log_index']] = {"value": message["value"], "message_id": message["message_id"]}
        # update the tickets available
        self.notify_client(message)
        print 'printing log', log
        if message["value"] == "RECONFIGURE":
            Proposer.number_of_connections += 1
            Proposer.majority = Proposer.number_of_connections / 2
            print "updated majority: ", Proposer.majority
        else:
            ticket_kiosk.sell_tickets(message["value"])

    def check_log(self, highest_index):
        try:
            for i in range(highest_index):
                if i+1 not in log.keys():
                    message = {"message_type": "SEND_LOG", "sender_id": process_id, "receiver_id": leader_id}
                    message_queue_lock.acquire()
                    message_queue.put(message)
                    message_queue_lock.release()
                    break
        except:
            print traceback.print_exc()

    def update_log(self, message):
        try:
            updated_log = message["log"]
            for key in updated_log:
                int_key = int(key)
                if int_key not in log:
                    if updated_log[key]['value'] != "RECONFIGURE":
                        ticket_kiosk.sell_tickets(updated_log[key]['value'])
                    log[int_key] = {'message_id': tuple(updated_log[key]['message_id']), 'value': updated_log[key]['value']}
                    self.notify_client(log[int_key])
            print "printing updated_log", log
        except:
            print traceback.print_exc()

    def notify_client(self, message):
        if ("message_id" in message) and message["message_id"][1] == process_id and message["value"] != "RECONFIGURE":
            reply_message = {"message_type": "SALE", "receiver_id": "client", "message_id": message['message_id'], "result": "success", "value": message["value"]}
            message_queue_lock.acquire()
            message_queue.put(reply_message)
            message_queue_lock.release()

    def handle_show_msg(self, message):
        reply_message = { "message_type" : "SHOW", "receiver_id" : "client", "tickets_available" : ticket_kiosk.get_available_tickets(), "log": log , "message_id":message["message_id"]}
        message_queue_lock.acquire()
        message_queue.put(reply_message)
        message_queue_lock.release()

    def handle_sale_failure(self, message):
        reply_message = {"message_type": "SALE", "receiver_id": "client", "message_id": message["message_id"],"result": "failure", "reason": "tickets not available"}
        message_queue_lock.acquire()
        message_queue.put(reply_message)
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
    unchosen_index = 0  # TODO choose uncommitted index
    prepared_msgs = []
    number_of_connections = 3
    majority = number_of_connections/2
    log_status = {}     # { log_index : "ballot_number":n, "value":val, "prepare_count":n, "accept_count":n, message_id}
    ballot_status = {}  # { log_index : "accept_num":n, "accept_val":val} TODO Not needed as separate dict?

    def send_prepare(self, message):
        try:
            #Proposer.get_uncommitted_index_and_ballot() # to not start with 0 as accept num and accept val have 0 values
            Proposer.get_uncommitted_index_and_ballot()
            log_index = Proposer.unchosen_index
            ballot_number = Proposer.ballot_number
            self.set_log_status(log_index, ballot_number, message)

            msg = {"message_type": "PREPARE", "ballot_number": ballot_number, "log_index": log_index,
                   "sender_id": process_id, "message_id": message["message_id"]}
            broadcast_msg(msg)
        except:
            print traceback.print_exc()

    def set_log_status(self, log_index, ballot_number, message):
        if log_index not in Proposer.log_status:
            Proposer.log_status[log_index] = {"prepare_count": 0, "accept_count": 0}
        if "value" in message:
            Proposer.log_status[log_index].update({"ballot_number": ballot_number, "value": message["value"],"message_id": message["message_id"]})
        else:
            Proposer.log_status[log_index].update({"ballot_number": ballot_number, "value": message["number_of_tickets"], "message_id": message["message_id"]})

    def receive_accept_prepare(self, msg):
        try:
            log_index = msg["log_index"]
            accept_num = msg["accept_num"]
            accept_val = msg["accept_val"]
            if log_index in Proposer.ballot_status:
                old_accept_num = Proposer.ballot_status[log_index]["accept_num"]
                if accept_num > old_accept_num:
                    Proposer.ballot_status[log_index].update({"accept_num": accept_num, "accept_val": accept_val, "message_id": msg["message_id"]})
            else:
                Proposer.ballot_status[log_index] = {"accept_num": accept_num, "accept_val": accept_val, "message_id": msg["message_id"]}
            if log_index in Proposer.log_status:
                Proposer.log_status[log_index]["prepare_count"] += 1

            self.check_prepare_status(log_index)
        except:
            print traceback.print_exc()

    def check_prepare_status(self, log_index):
        #accept num and val will be null when no conflicts
        if Proposer.log_status[log_index]["prepare_count"] == Proposer.majority:
            self.decide_value_to_accept(log_index)
            self.send_accept_msg(log_index)

    def decide_value_to_accept(self, log_index):

        if Proposer.ballot_status[log_index]["message_id"] > (0, 0):
            value = Proposer.ballot_status[log_index]["accept_val"]
            message_id = Proposer.ballot_status[log_index]["message_id"]
            if message_id in proposer.prepared_msgs:
                proposer.prepared_msgs.remove(message_id)
        else:
            value = Proposer.log_status[log_index]["value"]
            message_id = Proposer.log_status[log_index]["message_id"]

        Proposer.log_status[log_index]["value"] = value  # final value which will be committed
        Proposer.log_status[log_index]["message_id"] = message_id

    # Added flag when phase 1 runs as well to not increment twice
    def send_accept_msg(self, log_index):
        msg = { "message_type": "ACCEPT", "ballot_number": Proposer.log_status[log_index]["ballot_number"], "log_index": log_index, "value": Proposer.log_status[log_index]["value"],
                "message_id": Proposer.log_status[log_index]["message_id"], "sender_id": process_id}
        broadcast_msg(msg)

    def send_accept_msg_phase_2(self, message):
        Proposer.increase_indices()
        log_index = Proposer.unchosen_index
        self.set_log_status(log_index, Proposer.ballot_number, message)
        msg = {"message_type": "ACCEPT", "ballot_number": Proposer.log_status[log_index]["ballot_number"], "log_index": log_index, "value": Proposer.log_status[log_index]["value"],
                "message_id": Proposer.log_status[log_index]["message_id"], "sender_id": process_id}
        broadcast_msg(msg)

    def receive_ack(self, msg):
        log_index = msg["log_index"]
        if log_index in Proposer.log_status:
            Proposer.log_status[log_index]["accept_count"] += 1
        if msg["value"] == "RECONFIGURE" and msg["log_index"] in log:
            return
        self.check_log_status(log_index)

    def check_log_status(self, log_index):
        if Proposer.log_status[log_index]["accept_count"] == Proposer.majority:
            self.send_final_accept(log_index)

    def send_final_accept(self, log_index):
        try:
            ballot_number = Proposer.log_status[log_index]["ballot_number"]
            value = Proposer.log_status[log_index]["value"]
            message_id = Proposer.log_status[log_index]["message_id"]
            Proposer.remove_committed_message(message_id)
            log[log_index] = {"value": value, "message_id": message_id}
            msg = {"message_type": "COMMIT", "ballot_number": ballot_number, "log_index": log_index, "value": value, "sender_id": process_id, "message_id": message_id}
            broadcast_msg(msg)
            if value == "RECONFIGURE":
                Proposer.number_of_connections += 1
                Proposer.majority = Proposer.number_of_connections/2
                print "updated majority: ", Proposer.majority
            else:
                # update available tickets
                ticket_kiosk.sell_tickets(msg["value"])
            Proposer.set_leader_id()
            print 'printing log', log
            # reply to the client
            if message_id[1] == process_id:
                reply_message = {"message_type": "SALE", "receiver_id": "client", "message_id": message_id,
                                 "result": "success", "value": value}
                message_queue_lock.acquire()
                message_queue.put(reply_message)
                message_queue_lock.release()

        except:
            print '-----in commit------'
            print traceback.print_exc()

    @staticmethod
    def send_log(message):
        try:
            msg = {"message_type": "UPDATE_LOG", "log": log,
                   "sender_id": process_id, "receiver_id": message["sender_id"]}
            message_queue_lock.acquire()
            message_queue.put(msg)
            message_queue_lock.release()
        except:
            print traceback.print_exc()

    @staticmethod
    def remove_committed_message(message_id):
        request_queue_lock.acquire()
        delete_index = None
        for i in range(len(request_queue)):
            if request_queue[i]["message_id"] == message_id:
                delete_index = i
                break
        if delete_index is not None:
            del request_queue[delete_index]
        request_queue_lock.release()

    @staticmethod
    def set_leader_id():
        global leader_id
        if leader_id is None:
            leader_id = process_id

    @staticmethod
    def increase_indices():
        Proposer.ballot_number += 1
        Proposer.unchosen_index += 1

    @staticmethod
    def get_uncommitted_index_and_ballot():
        Proposer.unchosen_index = len(log)+1 # makes it start from 0 for now
        Proposer.ballot_number = highest_ballot_received + 1


def setup_receive_channels(s):
    while 1:
        try:
            conn, addr = s.accept()
            start_new_thread(receive_message, (conn,))
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
                client_soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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


def receive_message(socket):
    while True:
        try:
            message = socket.recv(4096)
            r = re.split('(\{.*?\})(?= *\{)', message)
            for msg in r:
                if msg == '\n' or msg == '' or msg is None:
                    continue
                msg = json.loads(msg)
                msg_type = msg["message_type"]
                if msg_type != "HEARTBEAT":
                    print msg
                if "message_id" in msg:
                    msg["message_id"] = tuple(msg["message_id"])
                if msg_type == "BUY":
                    handle_request(msg)
                elif msg_type == "PREPARE":
                    acceptor.receive_prepare(msg)
                elif msg_type == "ACCEPT-PREPARE":
                    proposer.receive_accept_prepare(msg)
                elif msg_type == "ACCEPT":
                    acceptor.receive_accept(msg)
                elif msg_type == "ACCEPT-ACCEPT":
                    proposer.receive_ack(msg)
                elif msg_type == "COMMIT":
                    acceptor.receive_final_value(msg)
                elif msg_type == "SEND_LOG":
                    proposer.send_log(msg)
                elif msg_type == "UPDATE_LOG":
                    acceptor.update_log(msg)
                elif msg_type == "HEARTBEAT":
                    heartbeat_queue_lock.acquire()
                    heartbeat_message_queue.put(msg)
                    heartbeat_queue_lock.release()
                elif msg_type == "SHOW":
                    acceptor.handle_show_msg(msg)
                elif msg_type == "RECONFIGURE":
                    print "reconfig received"
                    request_queue_lock.acquire()
                    request_queue.append(msg)
                    request_queue_lock.release()
        except:
            #print traceback.print_exc()
            #print 'in exception'
            time.sleep(1)
            continue


def handle_request(msg):
    if msg["number_of_tickets"] <= ticket_kiosk.get_available_tickets():
        if check_if_present_in_log(msg['message_id']):
            print 'returned withput anything'
            acceptor.notify_client(msg)
            return
        if process_id == leader_id:
            request_queue_lock.acquire()
            request_queue.append(msg)
            request_queue_lock.release()
        else:
            if leader_id is not None:
                msg["receiver_id"] = leader_id
                print 'sending to leader', msg
                message_queue_lock.acquire()
                message_queue.put(msg)
                message_queue_lock.release()
            else:
                proposer.send_prepare(msg)
    else:
        acceptor.handle_sale_failure(msg)


def check_if_present_in_log(message_id):
    for i in range(1,len(log)):
        if i in log and message_id == log[i]["message_id"]:
            print "matched ", message_id, log[i]["message_id"]
            return True
    return False


def process_request():
    while True:
        try:
            for i in range(len(request_queue)):
                msg_id = request_queue[i]["message_id"]
                if msg_id not in proposer.prepared_msgs:
                    print 'not present-->', msg_id
                    proposer.prepared_msgs.append(msg_id)
                    proposer.send_accept_msg_phase_2(request_queue[i])
        except:
            print "-------logging-------"
            print traceback.print_exc()


def broadcast_msg(message):
    for i in config["dc"]:
        if i != process_id:
            message_copy = dict(message)
            message_copy['receiver_id'] = i
            message_queue_lock.acquire()
            message_queue.put(message_copy)
            message_queue_lock.release()
            if message["message_type"] == "COMMIT":
                time.sleep(3)

def send_heartbeat():
    c = 0
    while True:
        try:
            if process_id == leader_id:
                msg = {"message_type": "HEARTBEAT", "sender_id": process_id, "highest_ballot_number": Proposer.ballot_number, "highest_log_index": (len(log)), "c": c}
                broadcast_msg(msg)
                time.sleep(5)
                c += 1
        except:
            #print traceback.print_exc()
            print 'send heartbeat stopped'


def receive_heartbeat():
    time.sleep(8)
    global leader_id, highest_ballot_received
    while True:
        if process_id != leader_id:
            try:
                if heartbeat_message_queue.qsize() > 0:
                    #heartbeat_queue_lock.acquire()
                    while heartbeat_message_queue.qsize() > 1:
                        heartbeat_message_queue.get()
                    msg = heartbeat_message_queue.get()
                    #heartbeat_message_queue.queue.clear()
                    #heartbeat_queue_lock.release()
                    leader_id = msg["sender_id"]
                    highest_ballot_received = msg["highest_ballot_number"]
                    highest_index = msg["highest_log_index"]
                    acceptor.check_log(highest_index)
                    print 'got heartbeat from with ballot and index and count', leader_id, highest_ballot_received, msg["highest_log_index"], msg["c"]
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
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #s.setblocking(0)
    s.bind((HOST, PORT))
    s.listen(10)

    start_new_thread(setup_receive_channels, (s,))
    start_new_thread(setup_send_channels, ())
    # t1 = threading.Thread(target=setup_send_channels, args=())
    # t1.start()
    start_new_thread(send_message, ())
    #start_new_thread(receive_message, ())
    start_new_thread(send_heartbeat, ())
    start_new_thread(receive_heartbeat, ())
    start_new_thread(process_request, ())

    proposer = Proposer()
    acceptor = Acceptor()
    ticket_kiosk = tickets.TicketKiosk()


while True:
    #pass the config msg here?
    client_input = raw_input()
    if "RECONFIGURE" in client_input:
        formatted_msg = { "message_type": "RECONFIGURE", "message_id": (1, process_id), "value" : "RECONFIGURE", "sender_id" : process_id }
        if leader_id is not None:
            print "here"
            formatted_msg["receiver_id"] = leader_id
            message_queue_lock.acquire()
            message_queue.put(formatted_msg)
            message_queue_lock.release()

    # if "BUY" in client_input:
    #     number_of_tickets = client_input.split(":")[1]
    #     message_id += 1
    #     if len(number_of_tickets) > 0:
    #         number_of_tickets = int(number_of_tickets)
    #         formatted_msg = {"message_type": "BUY", "number_of_tickets": number_of_tickets, "message_id": (message_id, process_id)}
    #         handle_request(formatted_msg)


