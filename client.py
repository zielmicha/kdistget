#!/usr/bin/python
import sys
import hashlib
import os
import functools
import collections
import socket
import threading
import select
import Queue

BLOCK_SIZE = 1024 * 1024

server = None

block_ptrs = collections.defaultdict(list)
valid_blocks = set()
invalid_blocks = set()

lock = threading.Lock()
data_lock = threading.Lock()
waiting_senders = 0
sent_need_msg = 0
waiting_need_blocks = set()

to_send_has = []

ack_queue = Queue.Queue()

MAX_NEED_MSG = 40
SENDER_THREADS = 5

def tracker_loop():
    global sent_need_msg, waiting_senders, to_send_has
    
    waiting_need_blocks.update(invalid_blocks)
    
    raw_sock = sock_connect(server)
    sock = raw_sock.makefile('r+', 1)
    
    sock.write('%d\n' % (port))
    
    to_send_has = valid_blocks
    
    while True:
        with lock:
            _to_send_has = to_send_has
            to_send_has = []
        
        
        for hash in _to_send_has:
            print 'has', hash
            sock.write('h%s\n' % hash)
        
        sock.flush()
        
        #print 'waiting_need_blocks', len(waiting_need_blocks), 'sent', sent_need_msg
        while waiting_need_blocks and sent_need_msg < MAX_NEED_MSG:
            with lock:
                hash = waiting_need_blocks.pop()
                sent_need_msg += 1
            print 'send need %s' % hash
            sock.write('n%s\n' % hash)
            sock.flush()
        
        while waiting_senders > 0:
            with lock:
                waiting_senders -= 1
            sock.write('g\n')
        
        sock.flush()
        
        r, _, _ = select.select([sock], [], [], 0.1)
        
        if r:
            cmd, rest = sock.readline().split(None, 1)
            if cmd == 'ack':
                client, hash = rest.split(None)
                print 'ack', client, hash
                ack_queue.put((client, hash))
            elif cmd == 'skip':
                with lock:
                    waiting_senders += 1
            else:
                print 'unknown command %r' % cmd

def sender_loop():
    global waiting_senders
    
    while True:
        with lock:
            waiting_senders += 1
        
        client, hash = ack_queue.get()
        
        print 'sending', hash, 'to', client
        
        ptrs = block_ptrs[hash]
        
        if not ptrs:
            print 'doesn\'t have block %s' % hash
        
        data = ptrs[0]()
        
        raw_sock = sock_connect(client)
        sock = raw_sock.makefile('r+')
        sock.write('%s\n' % hash)
        
        sock.write(data)
        sock.close()
        raw_sock.close()
        
        print 'sent!'

def listener_loop():
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', port))
    sock.listen(2)
    
    while True:
        client, addr = sock.accept()
        print 'incoming connection from', addr
        threading.Thread(target=handle_client, args=(client, )).start()
        del client

def handle_client(raw_sock):
    sock = raw_sock.makefile('r+')
    print 'handle_client'
    hash = sock.readline().strip()
    print 'incoming block', hash
    data = sock.read()
    
    add_incoming_data(hash, data)
    
    sock.close()

def add_incoming_data(hash, data):
    global sent_need_msg
    
    if make_hash(data) != hash:
        print 'got corrupt data!', hash
        return 
    
    with lock:
        if hash in invalid_blocks:
            invalid_blocks.remove(hash)
        to_send_has.append(hash)
        valid_blocks.add(hash)
        sent_need_msg -= 1
    
    for block_ptr in block_ptrs[hash]:
        block_ptr(data)
    
    print 'imported %s, invalid blocks left %d' % (hash, len(invalid_blocks))
    print 'send_need_msg = %d' % sent_need_msg

def main_loop():
    threading.Thread(target=tracker_loop).start()
    for i in xrange(SENDER_THREADS):
        threading.Thread(target=sender_loop).start()
    listener_loop()

def sock_connect(name):
    host, port = name.rsplit(':', 1)
    sock = socket.socket()
    sock.connect((host, int(port)))
    return sock

def make_hash(block):
    return hashlib.sha1(block).hexdigest()

def main_seed(port, fn):
    if os.path.exists(fn + '.kdg') and not is_kdg(fn + '.kdg'):
        sys.exit('%s.kdg exists and is not a kdg file - not overwriting')
    create_kdg(fn, fn + '.kdg')
    
    main_get(port, fn, fn + '.kdg')

def main_get(port_, datafn, fn):
    global port
    port = port_
    
    read_kdg(datafn, fn)
    read_blocks()
    print 'read valid blocks %d/%d' % (len(valid_blocks), len(valid_blocks) + len(invalid_blocks))
    
    main_loop()

def read_blocks():
    for hash, vals in block_ptrs.items():
        valids = []
        invalids = []
        
        for ptr in vals:
            if make_hash(ptr()) == hash:
                valids.append(ptr)
            else:
                invalids.append(ptr)
        
        #print 'for hash:', len(valids), 'valids', len(invalids), 'invalids'
        
        if valids:
            if invalids:
                data = valids[0]()
                for ptr in invalids:
                    print 'correcting invalid block for', hash
                    ptr(data)
            valid_blocks.add(hash)
        else:
            invalid_blocks.add(hash)

def read_kdg(datafn, fn):
    if not os.path.exists(datafn):
        open(datafn, 'w').close()
    dataf = open(datafn, 'rb+')
    kdg = open(fn)
    
    header = kdg.readline(50).rstrip()
    if header != '[kdistget]':
        raise IOError('invalid header')
    
    properties = {}
    
    for line in kdg:
        line = line.rstrip()
        if line == '[hashes]':
            break
        elif '=' not in line:
            raise IOError('invalid line %r' % line)
        else:
            key, val = line.split('=', 1)
            properties[key] = val
    
    global BLOCK_SIZE, server
    BLOCK_SIZE = int(properties['blocksize'])
    server = properties['server']
    size = int(properties['size'])
    
    dataf.truncate(size)
    
    def block_reader(i, write=None):
        with data_lock:
            dataf.seek(i * BLOCK_SIZE)
            if not write:
                return dataf.read(BLOCK_SIZE)
            else:
                assert len(write) <= BLOCK_SIZE
                dataf.write(write)
                dataf.flush()
    
    for i, hash in enumerate(kdg):
        hash = hash.rstrip()
        block_ptrs[hash].append(functools.partial(block_reader, i))

def is_kdg(name):
    return open(name).readline(50).rstrip() == '[kdistget]'

def create_kdg(src, dst):
    inp = open(src, 'rb')
    out = open('%s.tmp~' % dst, 'w')
    out.write('[kdistget]\n')
    out.write('blocksize=%d\n' % BLOCK_SIZE)
    out.write('server=%s\n' % server)
    out.write('size=%d\n' % os.path.getsize(src))
    out.write('[hashes]\n')
    while True:
        data = inp.read(BLOCK_SIZE)
        if not data:
            break
        out.write('%s\n' % make_hash(data))
    out.close()
    os.rename('%s.tmp~' % dst, dst)


if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) < 3 or (args[1] == '--seed' and len(args) != 4):
        sys.exit('usage: client.py port (datafile kdgfile | --seed datafile server)')
    
    port = int(args[0])
    
    if args[1] == '--seed':
        server = args[3]
        main_seed(port, args[2])
    else:
        main_get(port, args[1], args[2])