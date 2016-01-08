import sys, socket, json, time, signal
from select import select
from collections import defaultdict, namedtuple
from threading import Thread, Timer
from datetime import datetime
from copy import deepcopy


##---------------------------------------------
RECV_SOCK_BUFFER = 4096
INFINITY = float('inf')
self_id = ""
#neighbor --> routing table
neighbors = {}      
#node --> dict{'cost': ..., 'link': ...}
routing_table = {}  
#neighbor_id --> edge cost of each link
adjacent_links = {} 
#active neighbor nodes --> link, cost
old_links = {}    
timer_log = {}
#list of pair of (node1, node2), one takes another offline
dead_links = []   
##----------------------------------------------

recvSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
recvSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

def main():
    global self_id
    if (len(sys.argv) < 3):
        print "Usage: client.py <localport> <timeout> <<id_addr1> <port1> <weight1>>..."
        sys.exit()

    localport = int(sys.argv[1])
    recvSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    recvSock.bind(('', localport))
    signal.signal(signal.SIGINT,signal_handler)
    host = socket.gethostbyname(socket.gethostname())
    self_id = str(host) + ":" + sys.argv[1]
    time_out = int(sys.argv[2])


    if ((len(sys.argv) - 3) % 3) != 0:
        print "ERROR: missing link inputs"
        sys.exit()

    for i in range(3, len(sys.argv) - 1, 3):
        neighbor_ip = socket.gethostbyname(sys.argv[i])
        neighbor_id = str(neighbor_ip) + ":" + sys.argv[i + 1]
        routing_table[neighbor_id] = {}
        routing_table[neighbor_id]['cost'] = float(sys.argv[i + 2])
        routing_table[neighbor_id]['link'] = neighbor_id
        adjacent_links[neighbor_id] = float(sys.argv[i + 2])
        #right now neighbors should be empty
        neighbors[neighbor_id] = {}

    #for testing purpose only
    print "still running at line 58"

    update_timer(time_out)
    timer_thread(time_out)
    sys.stdout.flush()

    while True:
        sockets = [sys.stdin, recvSock]
        try:
            read_sockets, write_sockets, error_sockets = select(sockets,[],[])
        except select.error, e:
            break
        except socket.error, e:
            break

        for sock in read_sockets:
            if sock == recvSock:
                data, addr = recvSock.recvfrom(RECV_SOCK_BUFFER)
                if data:
                    msg = json.loads(data)
                    #msg is a json object witht 
                    msg_parser(msg, addr)
                else:
                    print "ERROR: supposed to receive data"
            else:
                line = sys.stdin.readline()
                data = line.rstrip()
                if len(data) > 0:
                    data_list = data.split()
                    cmd_parser(data_list)
                    sys.stdout.flush()
                else:
                    sys.stdout.flush()

    recvSock.close()




#-------PARSING COMMAND LINES--------#
def cmd_parser(args):
    if args[0] == "LINKDOWN":
        if len(args) == 3:
            linkdown(args[1], args[2])
        else:
            print "cmd ERROR: LINKDOWN: incorrect # of args!!!"

    elif args[0] == "LINKUP":
        if len(args) == 3:
            linkup(args[1], args[2])
        else:
            print "cmd ERROR: LINKUP incorrect # of args!!!"

    elif args[0] == "SHOWRT":
        showrt()

    elif args[0] == "CLOSE":
        sys.exit("[%s] CLOSE EXECUTED, PROCESS KILLED" % self_id)

#--------FUNCTIONS OF COMMANDS
def linkdown(ip_addr, port):
    global self_id
    node_id = str(ip_addr) + ":" + str(port)
    if node_id not in neighbors:
        print "ERR: %s is not a neighbor." % node_id
    else:
        cost = adjacent_links[node_id]
        old_links[node_id] = cost

        if routing_table[node_id]['cost'] != INFINITY:
            routing_table[node_id]['cost'] = INFINITY
            routing_table[node_id]['link'] = "NULL"

        for node in routing_table:
            if routing_table[node]['link'] == node_id:
                if node not in neighbors:
                    routing_table[node]['cost'] = INFINITY
                    routing_table[node]['link'] = "NULL"
                else:
                    routing_table[node]['cost'] = adjacent_links[node]
                    routing_table[node]['link'] = node

        pair_key = str(self_id) + "," + str(node_id)
        #dead_links is a list of <node1<ID>, node2<ID>>
        dead_links.append(pair_key)
        send_dict1 = { 'source': 'linkdown', 'pair': pair_key }
        serialized_dict = json.dumps(send_dict1)
        for neighbor in neighbors:
            temp = neighbor.split(":")
            recvSock.sendto(serialized_dict, (temp[0], int(temp[1])))

        del neighbors[node_id]

def linkup(ip_addr, port):
    global self_id
    node_id = str(ip_addr) + ":" + str(port)
    if node_id not in old_links:
        print "ERR from line 429, link doesn't exist"

    else:
        routing_table[node_id]['cost'] = old_links[node_id]
        del old_links[node_id]
        routing_table[node_id]['link'] = node_id
        neighbors[node_id] = {} # initialize table, wait for update

        #either pair1 or pair2 will be in the dead_links lists
        pair_one = self_id + "," + node_id
        pair_two = node_id + "," + self_id

        if (pair_one)  in dead_links:
            dead_links.remove(pair_one)
        elif (pair_two) in dead_links:
            dead_links.remove(pair_two)

        dict_temp = { 'source': 'linkup', 'pair': pair_one, }
        serialized_dict = json.dumps(dict_temp)
        for neighbor in neighbors:
            temp = neighbor.split(":")
            recvSock.sendto(serialized_dict, (temp[0], int(temp[1])))


def showrt():
    print time.strftime("ROUTING TABLE AT <%H-%M-%S> ")
    print "<destination>\t\t<cost>\t\t<link>"
    for node in routing_table:
        link = routing_table[node]['link']
        #s = []
        #print str(node) + "\t" + str(routing_table[node]['cost'] + "\t" + link)
        print "[" + str(node) + "]\t" + str(routing_table[node]['cost']) + "\t\t" + '[' + link + ']'
#------handling incoming data-------------
def msg_parser(rcv_data, tuple_addr):
    global self_id
    table_changed = False
    t_now = int(time.time())
    addr = str(tuple_addr[0]) + ":" + str(tuple_addr[1])
    #not sure if it's the right thing to do...
    if rcv_data['source'] == 'update':
        msg_update(rcv_data, addr, t_now)
    elif rcv_data['source'] == 'linkup':
        msg_linkup(rcv_data, addr, t_now)
    elif rcv_data['source'] == 'linkdown':
        msg_linkdown(rcv_data, addr, t_now)
    elif rcv_data['source'] == 'close':
        msg_close(rcv_data, addr, t_now)
    else:
        print 'Error in data transmission from peer %s' %addr

def msg_update(rcv_data, addr, current_time):
        timer_log[addr] = current_time
        # update our existing neighbor table for this address
        count = 0
        if addr in neighbors:
            neighbors[addr] = rcv_data['routing_table']

        if addr in routing_table:
            if routing_table[addr]['cost'] == INFINITY:
                routing_table[addr]['cost'] = adjacent_links[addr]
                routing_table[addr]['link'] = addr
                count += 1
                if addr in adjacent_links:
                    neighbors[addr] = rcv_data['routing_table']

        elif rcv_data['routing_table'].has_key(self_id):
            routing_table[addr] = {}
            routing_table[addr]['cost'] = rcv_data['routing_table'][self_id]['cost']
            routing_table[addr]['link'] = addr
            count += 1

            # new node enters the network as immediate neighbor
            if rcv_data['routing_table'][self_id]['link'] == self_id:
                neighbors[addr] = rcv_data['routing_table']
                adjacent_links[addr] = rcv_data['routing_table'][self_id]['cost']
        else:
                sys.exit("not sure what happened... go to line 214")

        for node in rcv_data['routing_table']:
            if node != self_id:
                if node not in routing_table:
                    routing_table[node] = {'cost': INFINITY,'link': "NULL"}
                    count += 1
                # --- BELLMAN FORD ALGORITHM --- #
                for dest in routing_table:
                    old_cost = routing_table[dest]['cost']
                    if addr in neighbors and dest in neighbors[addr]:
                        new_cost = routing_table[addr]['cost'] + neighbors[addr][dest]['cost']

                        if new_cost < old_cost:
                            routing_table[dest]['cost'] = new_cost
                            routing_table[dest]['link'] = addr
                            count += 1

            if count > 0:
                broadcast_routing_table()

def msg_linkup(rcv_data, addr, current_time):
    count = 0
    timer_log[addr] = current_time
    pair = rcv_data['pair']
    temp = pair.split(',')
    pair_reverse = str(temp[1]) + "," + str(temp[0])
    if pair in dead_links:
        dead_links.remove(pair)
        count += 1
    elif pair_reverse in dead_links:
        dead_links.remove(pair_reverse)
        count += 1

    if count > 0:
        if temp[0] == addr and temp[1] == self_id:
            routing_table[addr]['cost'] = old_links[addr]
            routing_table[addr]['link'] = addr
            neighbors[addr] = {}
            del old_links[addr]
            send_dict = { 'type': 'linkup', 'pair': pair, }
            serialized_dict = json.dumps(send_dict)
            for neighbor in neighbors:
                temp = neighbor.split(":")
                recvSock.sendto(serialized_dict, (temp[0], int(temp[1])))
    else:
        broadcast_routing_table()

def msg_linkdown(rcv_data, addr, current_time):
    timer_log[addr] = current_time
    count = 0
    pair = rcv_data['pair']
    if pair in dead_links:
        broadcast_routing_table()

    else:
        dead_links.append(pair)
        temp = pair.split(',')
        if temp[0] == addr and temp[1] == self_id: 
            old_links[addr] = adjacent_links[addr]
            routing_table[addr]['cost'] = INFINITY
            routing_table[addr]['link'] = "NULL"
            if addr in neighbors:
                del neighbors[addr]
        for node in routing_table:
            if node in neighbors:
                routing_table[node]['cost'] = adjacent_links[node]
                routing_table[node]['link'] = node

            else:
                routing_table[node]['cost'] = INFINITY
                routing_table[node]['link'] = "NULL"

        send_dict = {'source': 'linkdown', 'pair': pair, }
        serialized_dict = json.dumps(send_dict)
        for neighbor in neighbors:
            temp = neighbor.split(":")
            recvSock.sendto(serialized_dict, (temp[0], int(temp[1])))

def msg_close(rcv_data, addr, current_time):
    timer_log[addr] = current_time
    close_node = rcv_data['target']
    if routing_table[close_node]['cost'] != INFINITY:
        routing_table[close_node]['cost'] = INFINITY
        routing_table[close_node]['link'] = "NULL"

        if close_node in neighbors:
            del neighbors[close_node]

        for node in routing_table:
            if node not in neighbors:
                routing_table[node]['cost'] = INFINITY
                routing_table[node]['link'] = "NULL"
                send_dict = {'source': 'close', 'target': close_node, }
                serialized_dict = json.dumps(send_dict)
                for neighbor in neighbors:
                    temp = neighbor.split(":")
                    recvSock.sendto(serialized_dict, (temp[0], int(temp[1])))
            else:
                routing_table[node]['cost'] = adjacent_links[node]
                routing_table[node]['link'] = node
            
    else:
        broadcast_routing_table()    

#broaccast tourting table to neighbors
def broadcast_routing_table(): 
    #do i need to use deepcopy?
    neighbors_dpcopy = deepcopy(neighbors)
    for neighbor in neighbors_dpcopy:
        temp = neighbor.split(':')
        addr = (temp[0], int(temp[1]))
        send_dict = {'source': 'update', 'routing_table': {}, }
        rt_dpcopy = deepcopy(routing_table)
        for node in rt_dpcopy: 
            send_dict['routing_table'][node] = rt_dpcopy[node]
            if node != neighbor and rt_dpcopy[node]['link'] == neighbor:
                send_dict['routing_table'][node]['cost'] = INFINITY
        recvSock.sendto(json.dumps(send_dict), addr)

def update_timer(timeout_arg):
    broadcast_routing_table()
    t = Timer(timeout_arg, update_timer, [timeout_arg])
    t.setDaemon(True)
    t.start()

# check if time is out
def timer_thread(timeout_arg):
    for neighbor in deepcopy(neighbors):
        if neighbor in timer_log:
            t_threshold = (3 * timeout_arg)
            
            if ((int(time.time()) - timer_log[neighbor]) > t_threshold):
                if routing_table[neighbor]['cost'] == INFINITY:
                    broadcast_routing_table()
                else:
                    routing_table[neighbor]['cost'] = INFINITY
                    routing_table[neighbor]['link'] = "NULL"
                    del neighbors[neighbor]
                    
                    for node in routing_table:
                        if node in neighbors:
                            routing_table[node]['cost'] = adjacent_links[node]
                            routing_table[node]['link'] = node
                        else:
                            routing_table[node]['cost'] = INFINITY
                            routing_table[node]['link'] = "NULL"

                    send_dict = { 'source': 'close', 'target': neighbor }
                    for neighbor in neighbors:
                        temp = neighbor.split(':')
                        recvSock.sendto(json.dumps(send_dict), (temp[0], int(temp[1])))

    timer = Timer(3, timer_thread, [timeout_arg])
    timer.setDaemon(True)
    timer.start()

#---------utility
def signal_handler(signal, frame):
     sys.exit(0)

if __name__ == "__main__":
    main()