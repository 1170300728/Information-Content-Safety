#!/usr/bin/env python
# encoding: utf-8

import socket
from hashlib import sha1
from random import randint
from struct import unpack
from socket import inet_ntoa
from threading import Thread
from time import sleep
from collections import deque
from bencode import bencode, bdecode
from database import mydatabase
from log import Logger
import os
import sys

sys.setrecursionlimit(10000)

BOOTSTRAP_NODES = (

    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),

)

TID_LENGTH = 2
RE_JOIN_DHT_INTERVAL = 3
TOKEN_LENGTH = 2
WAIT_TIME = 10


def entropy(length):
    return "".join(chr(randint(0, 255)) for _ in range(length))


def random_id(num):
    return os.urandom(num)


def decode_nodes(nodes):
    n = []
    length = len(nodes)
    if (length % 26) != 0:
        return n

    for i in range(0, length, 26):
        nid = nodes[i:i + 20]
        ip = inet_ntoa(nodes[i + 20:i + 24])
        port = unpack("!H", nodes[i + 24:i + 26])[0]
        n.append((nid, ip, port))

    return n


def get_neighbor(target, nid, end=14):
    return target[:end] + nid[end:]


class KNode(object):

    def __init__(self, nid, ip, port):
        self.nid = nid
        self.ip = ip
        self.port = port


class DHT:
    def __init__(self, bind_ip, bind_port, max_node_qsize):
        self.max_node_qsize = max_node_qsize
        self.nid = random_id(20)
        print(str(self.nid))
        self.nodes = deque(maxlen=max_node_qsize)
        self.outflag = False
        self.bind_ip = bind_ip
        self.bind_port = bind_port
        self.database = mydatabase()
        self.log = Logger('all.log', level='debug')
        self.rebuildstart = False
        self.rightnode = []
        self.send_endflag = False
        self.countgo = 0
        self.cheatlist = []
        self.min_distance = -1
        self.min_distance_list = []

        self.process_request_actions = {
            b"get_peers": self.on_get_peers_request,
            b"announce_peer": self.on_announce_peer_request,
        }

        self.ufd = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.ufd.settimeout(10)
        self.ufd.bind((self.bind_ip, self.bind_port))

    def send_krpc(self, msg, address):
        try:
            self.ufd.sendto(bencode(msg), address)
        except:
            pass

    def send_ping(self, address):
        tid = random_id(2)
        msg = {
            "t": tid,
            "y": "q",
            "q": "ping",
            "a": {
                "id": self.nid,
            }
        }
        self.log.write("向%s发送ping报文" % str(address))
        self.send_krpc(msg, address)

    def send_find_node(self, address, nid=None):
        nid = get_neighbor(nid, random_id(20)) if nid else self.nid

        tid = random_id(2)
        msg = {
            "t": tid,
            "y": "q",
            "q": "find_node",
            "a": {
                "id": nid,
                "target": random_id(20)
            }
        }
        self.log.write("向%s发送find_node报文" % str(address))
        self.send_krpc(msg, address)

    def join_DHT(self):
        self.log.write("尝试加入DHT网络")
        for address in BOOTSTRAP_NODES:
            self.send_find_node(address)
            print(str(address))
            # sleep(10)

    def re_join_DHT(self):
        if len(self.nodes) == 0:
            self.join_DHT()
        # sleep(WAIT_TIME)

    def send_loop(self):
        wait = 1.0 / self.max_node_qsize
        sleep(2)
        self.log.write("send_loop线程启动")
        print("send_loop start")
        self.outcount = self.max_node_qsize * 0.5
        while True:
            if len(self.nodes) > self.outcount:
                self.outflag = True
                self.ufd.close()
                self.database.myclose()
                print("send_loopdht" + str(len(self.nodes)))
                break
            try:
                node = self.nodes.popleft()
                self.send_find_node((node.ip, node.port), node.nid)
                self.nodes.append(node)
            except IndexError:
                sleep(WAIT_TIME)
                self.re_join_DHT()
            sleep(wait)

    def process_find_node_response(self, msg, address):
        nodes = decode_nodes(msg[b"r"][b"nodes"])
        for node in nodes:
            (nid, ip, port) = node
            if len(nid) != 20:
                continue
            if ip == self.bind_ip:
                continue
            if port < 1 or port > 65535:
                continue
            n = KNode(nid, ip, port)
            self.database.myinsert(nid, ip, port)
            print("databasemyinsert" + str(len(self.nodes)), end=" : ")
            self.log.write('成功获取nodeid为"%s"的结点信息' % nid.hex())
            self.nodes.append(n)
        print(1)

    def recv_loop(self):
        # self.join_DHT()
        self.log.write("recv_loop线程启动")
        print("recv_loop start")
        while True:
            if self.outflag:
                break
            try:
                data, address = self.ufd.recvfrom(65535)
                msg = bdecode(data)
                self.on_message(msg, address)
            except Exception as e:
                self.log.write("recv错误：" + str(e))
                print("203" + str(e))

    def on_message(self, msg, address):
        try:
            if msg[b"y"] == b"r":
                if b"nodes" in msg[b"r"]:
                    self.process_find_node_response(msg, address)
            elif msg[b"y"] == b"q":
                try:
                    self.process_request_actions[msg[b"q"]](msg, address)
                except KeyError:
                    self.play_dead(msg, address)
        except KeyError:
            pass

    def on_get_peers_request(self, msg, address):
        try:
            infohash = msg[b"a"][b"info_hash"]
            tid = msg[b"t"]
            nid = msg[b"a"][b"id"]
            token = infohash[:TOKEN_LENGTH]
            msg = {
                "t": tid,
                "y": "r",
                "r": {
                    "id": get_neighbor(infohash, self.nid),
                    "nodes": "",
                    "token": token
                }
            }
            self.log.write("获取了get_peers报文")
            self.send_krpc(msg, address)
        except KeyError:
            pass

    def on_announce_peer_request(self, msg, address):
        try:
            infohash = msg[b"a"][b"info_hash"]
            # print msg["a"]
            tname = msg[b"a"][b"name"]
            token = msg[b"a"][b"token"]
            nid = msg[b"a"][b"id"]
            tid = msg[b"t"]

            if infohash[:TOKEN_LENGTH] == token:
                if b"implied_port" in msg[b"a"] and msg[b"a"][b"implied_port"] != 0:
                    port = address[1]
                else:
                    port = msg[b"a"][b"port"]
                    if port < 1 or port > 65535:
                        return
        except Exception:
            pass
        finally:
            self.ok(msg, address)

    def play_dead(self, msg, address):
        try:
            # tid = bytes.decode(msg[b"t"])
            tid = msg[b"t"]
            msg = {
                "t": tid,
                "y": "e",
                "e": [202, "Server Error"]
            }
            self.send_krpc(msg, address)
        except KeyError:
            pass

    def ok(self, msg, address):
        try:
            tid = msg[b"t"]
            nid = msg[b"a"][b"id"]
            msg = {
                "t": tid,
                "y": "r",
                "r": {
                    "id": get_neighbor(nid, self.nid)
                }
            }
            self.log.write("获取到announce_peer报文")
            self.send_krpc(msg, address)
        except KeyError:
            pass
