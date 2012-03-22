#!/usr/bin/python
from __future__ import print_function

import socket
import sys
import threading
import time
import os

clients = {}
lock = threading.Lock()

try:
    import signal
    signal.signal(signal.SIGINT, lambda a,b: os._exit(1))
except ImportError:
    pass

def d(*s):
    pass #print(*s)

def log(*s):
    print(*s)

def wtf(*s):
    print('WTF:', *s)

class Client:
    def __init__(self, host):
        self.host = host
        self.has = set()
        self.need = set()
        
        self.sending = set() # to restore after client dies

def main(port):
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', port))
    sock.listen(2)
    
    
    while True:
        client, addr = sock.accept()
        d('incoming connection from', addr)
        threading.Thread(target=handle_client, args=(client, addr[0])).start()
        del client

def handle_client(raw_sock, client_ip):
    sock = raw_sock.makefile('r+', 1)
    
    port = sock.readline().strip()
    host = '%s:%s' % (client_ip, port)
    
    log('connected', host)
    
    client = clients[host] = Client(host)
    
    try:
        while True:
            line = sock.readline().strip()
            
            if not line:
                return
            
            d(host, repr(line))
            if line[0] == 'h':
                with lock:
                    hash = line[1:]
                    client.has.add(hash)
                    if hash in client.need:
                        client.need.remove(hash)
            elif line[0] == 'n':
                with lock:
                    client.need.add(line[1:])
            elif line[0] == 'g':
                res = None
                timeout = 10
                while not res:
                    #print 'finding pair'
                    res = find_pair_and_save(client)
                    time.sleep(0.07)
                    timeout -= 1
                    if not timeout:
                        break
                
                if not res:
                    sock.write('skip -\n')
                    sock.flush()
                else:
                    other, hash = res
                    d('requesting', host, 'to send', hash, 'to', other)
                    sock.write('ack %s %s\n' % (other, hash))
                    sock.flush()
            else:
                wtf('unknown command', line[0])
    except socket.error:
        pass
    finally:
        with lock:
            for to, hash in client.sending:
                to.need.add(hash)
            del clients[host]
        
        log('disconnect %s' % host)

def find_pair_and_save(client):
    with lock:
        for other in clients.values():
            if other == client: continue
            
            good = other.need.intersection(client.has)
            #print 'need of %s: %s' % (other.host, other.need)
            if good:
                hash = good.pop()
                client.sending.add((other, hash))
                other.need.remove(hash)
                return other.host, hash
        
        return None

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit('usage: tracker.py port')
    main(int(sys.argv[1]))