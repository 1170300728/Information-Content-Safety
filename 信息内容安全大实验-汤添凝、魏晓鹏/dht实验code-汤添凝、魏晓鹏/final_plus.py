#!/usr/bin/env python
# encoding: utf-8
import tkinter
from threading import Thread
from time import *
from collections import deque
from tkinter.ttk import Treeview
import tkinter.font as tkFont
from bencode import bencode, bdecode
from database import mydatabase
from log import Logger
from dht import DHT, KNode, decode_nodes, random_id
import importlib, sys

importlib.reload(sys)

import xlwt
import pymysql

import sys
from tkinter import *

sys.setrecursionlimit(10000)

WAIT_TIME = 3
WAIT_TIME_NEXT = 0.1


class BucketInfo:
    def __init__(self, minedge, maxedge, outminedge, outmaxedge, sendid):
        self.minedge = minedge
        self.maxedge = maxedge
        self.outminedge = outminedge
        self.outmaxedge = outmaxedge
        self.sendid = sendid


class RebuildTool(DHT):
    def send_find_node_rebuild(self, address, findid=None):
        nid = self.nid
        target = findid
        tid = random_id(2)
        msg = {
            "t": tid,
            "y": "q",
            "q": "find_node",
            "a": {
                "id": nid,
                "target": target
            }
        }
        self.log.write("向%s发送find_node报文" % str(address))
        self.send_krpc(msg, address)

    def rebuild_routing_list(self):
        # rebuildnode = self.database.myselect(nid)
        # if rebuildnode == None:
        #     print("Try Another node please!")
        #     return

        rebuildnum = 10
        while True:
            flag = 1
            try:
                if self.targetid:
                    if self.targetid.replace("\n", "") in self.cheatlist:
                        sleep(15)
                        self.outflag = True
                        return

                    for n in self.nodes:
                        print(str(n.nid.hex()))
                        if str(n.nid.hex()) == self.targetid.replace("\n", ""):
                            rebuildnum = self.nodes.index(n)
                            flag = 0
                            break
            except Exception as e:
                print("59" + str(e))
            sleep(1)
            if flag == 0:
                break

        ifping = True
        self.ifnext = False
        count = 0
        while True:
            if ifping:
                self.rebuild_node = self.nodes[rebuildnum]
                self.rebuildaddress = (self.rebuild_node.ip, self.rebuild_node.port)
                self.send_ping(self.rebuildaddress)
                rebuildnum += 1
                ifping = False
            elif self.ifnext:
                self.rebuildid = self.rebuild_node.nid
                self.rebuildroutinglist = self.construct_rebuild_send_list(self.rebuildid)
                break
            else:
                if count == 25:
                    self.send_ping(self.rebuildaddress)
                if count >= 50:
                    # print(str(self.rebuild_node.nid)+"ping不通,跳过")
                    self.text2.insert(INSERT, str(self.rebuild_node.nid.hex()) + " ping不通" + "\n")
                    count = 0
                    ifping = True
                    continue
                sleep(WAIT_TIME_NEXT)
                count += 1
        self.neednext = True
        self.listnum = 0
        count = 0

        while True:
            if self.outflag or count >= 1:
                sleep(8)
                self.outflag = True

                break
            if self.neednext:
                for i in range(len(self.rebuildroutinglist)):
                    if i < self.min_distance - 1:
                        continue
                    self.send_find_node_rebuild(self.rebuildaddress, self.rebuildroutinglist[i])
                    print("asdasd" + str(i) + "  " + bin(
                        int(self.rebuildroutinglist[i].hex(), 16) ^ int(self.rebuildid.hex(), 16)))

                    tempcount = 0
                    while self.min_distance < 0:
                        sleep(1)
                        tempcount += 1
                        if tempcount % 5 == 0:
                            self.send_find_node_rebuild(self.rebuildaddress, self.rebuildroutinglist[i])
                    sleep(WAIT_TIME_NEXT)

                count += 1
                self.neednext = False
                self.count = 2
            else:
                if self.count > 1:
                    self.count -= 1
                    for bucket in self.rebuildroutinglist:
                        self.send_find_node_rebuild(self.rebuildaddress, bucket)
                        sleep(WAIT_TIME_NEXT)
                    count += 1
                else:
                    self.outflag = True
            sleep(WAIT_TIME)

    def build_hex_id(self, intedge):
        hexid = hex(intedge)[2:]
        if len(hexid) < 40:
            hexid = '0' * (40 - len(hexid)) + hexid
        return hexid

    # def construct_rebuild_send_list(self, nid):
    #     int_id = int(nid.hex(), 16)
    #     length = 160
    #     rebuildsendlist = []
    #     smalledge = 0
    #     bigedge = pow(2, 159)
    #     while True:
    #         if length <= 8:
    #             break
    #         if int_id >= bigedge:
    #             nextid = self.build_hex_id(smalledge)
    #             minedge = self.build_hex_id(smalledge)
    #             maxedge = self.build_hex_id(bigedge - 1)
    #             outminedge = self.build_hex_id(bigedge)
    #             outmaxedge = self.build_hex_id(bigedge - smalledge + bigedge - 1)
    #             rebuildsendlist.append(BucketInfo(minedge, maxedge, outminedge, outmaxedge, bytes.fromhex(nextid)))
    #             tempedge = bigedge
    #             bigedge = bigedge + (bigedge - smalledge) // 2
    #             smalledge = tempedge
    #             length -= 1
    #         else:
    #             nextid = self.build_hex_id(bigedge)
    #             minedge = self.build_hex_id(bigedge)
    #             maxedge = self.build_hex_id(bigedge - smalledge + bigedge - 1)
    #             outminedge = self.build_hex_id(smalledge)
    #             outmaxedge = self.build_hex_id(bigedge - 1)
    #             rebuildsendlist.append(BucketInfo(minedge, maxedge, outminedge, outmaxedge, bytes.fromhex(nextid)))
    #             bigedge = smalledge + (bigedge - smalledge) // 2
    #             length -= 1
    #     return rebuildsendlist

    def construct_rebuild_send_list(self, nid):
        int_id = int(nid.hex(), 16)
        rebuildsendlist = []
        self.printlist = []
        for i in range(159):
            rebuildsendlist.append(bytes.fromhex(self.build_hex_id(int_id ^ (pow(2, i) + pow(2, i + 1)))))
            self.printlist.append(self.build_hex_id(int_id ^ (pow(2, i) + pow(2, i + 1))))
        # rebuildsendlist.reverse()
        return rebuildsendlist

    def send_loop(self):
        wait = 1.0 / self.max_node_qsize
        sleep(2)
        self.log.write("send_loop线程启动")
        print("send_loop start")
        self.join_DHT()
        self.outcount = self.max_node_qsize * 0.01
        sum_t = 0.0
        sum_t1 = 0
        oldnum = 0

        while True:
            if len(self.nodes) > self.outcount:
                self.min_distance = -1
                self.rebuildstart = True
                self.rebuild_routing_list()
                break
            try:

                if len(self.nodes) > 0:
                    if oldnum != len(self.nodes):
                        sum_t = 0

                if sum_t >= 10:
                    oldnum = 0
                    sum_t = 0
                    sum_t1 = 0
                    self.nodes.clear()

                node = self.nodes.popleft()
                time_start = perf_counter()
                self.send_find_node((node.ip, node.port), node.nid)
                self.nodes.append(node)

                oldnum = len(self.nodes)
                sleep(wait)
                time_end = perf_counter()

                sum_t += (time_end - time_start)

            except IndexError:
                sleep(2)
                self.re_join_DHT()
                sleep(wait)

    # dht server
    def recv_loop(self):
        self.log.write("recv_loop线程启动")
        print("recv_loop start")
        while True:
            if self.outflag: break
            try:
                data, address = self.ufd.recvfrom(65535)
                msg = bdecode(data)
                if self.rebuildstart:
                    # print("skip")
                    self.on_message_rebuild(msg, address)
                else:
                    self.on_message(msg, address)
            except Exception as e:
                self.log.write("recv错误：" + str(e))
                print("161" + str(e))

    def startPage_loop(self):
        while True:
            if self.rebuildstart:
                self.window1()
                break
            sleep(1)

    def resultPage_loop(self):
        while True:
            if self.outflag:
                self.window2()
                break
            sleep(1)

    def on_message_rebuild(self, msg, address):
        if address == self.rebuildaddress:
            self.ifnext = True
            try:
                if msg[b"y"] == b"r":
                    if b"nodes" in msg[b"r"]:
                        self.process_find_node_response_rebuild(msg, address)
                elif msg[b"y"] == b"q":
                    try:
                        self.process_request_actions[msg[b"q"]](msg, address)
                    except KeyError:
                        self.play_dead(msg, address)
            except KeyError:
                pass
        else:
            try:
                if msg[b"y"] == b"q":
                    try:
                        self.process_request_actions[msg[b"q"]](msg, address)
                    except KeyError:
                        self.play_dead(msg, address)
            except KeyError:
                pass

    def process_find_node_response_rebuild(self, msg, address):
        nodes = decode_nodes(msg[b"r"][b"nodes"])
        need = False
        templist = []
        for node in nodes:
            (nid, ip, port) = node
            if len(nid) != 20:
                continue
            if ip == self.bind_ip:
                continue
            if port < 1 or port > 65535:
                continue
            # print(len(bin(int(nid.hex(),16)^int(self.rebuildid.hex(),16)))-2)
            templist.append(len(bin(int(nid.hex(), 16) ^ int(self.rebuildid.hex(), 16))) - 2)
            insertsuccess = self.database.myinsert_rebuild(nid, ip, port, self.rebuildid)
            if insertsuccess:
                need = True
                self.log.write('成功获取nodeid为"%s"的结点信息' % nid.hex())
            else:
                self.log.write('nodeid为"%s"的结点已被重复获取' % nid.hex())

        if need: self.neednext = True
        if self.min_distance < 0:
            templist.sort()
            self.min_distance = templist[-1]

    def window1(self):
        root = Tk()  # 创建窗口对象的背景色
        root.geometry("355x800")
        self.database = mydatabase()
        # list = self.database.selectDHTNode()
        print("=============================")
        text = Text(root, width=50, height=40)  # 一行10个字符，共2行
        listnode = list(self.nodes)
        for item in listnode:  # 第一个小部件插入数据
            # print(item)
            text.insert(INSERT, str(item.nid.hex()) + "\n")

        label = Label(root, text='input your choice')
        label.pack()
        self.targetid = None

        input = Text(root, width=50, height=1)
        input.pack()

        button = Button(root, text='go', command=lambda: self.abc(str=input.get('0.0', 'end')))  # 点击后会输出hello
        button.pack()

        text.pack()

        label2 = Label(root, text='error info:')
        label2.pack()

        self.text2 = Text(root, width=50, height=10)
        self.text2.pack()

        print(2)
        root.mainloop()  # 进入消息循环
        print(1)

    def abc(self, str):
        self.countgo += 1
        self.targetid = str.rstrip('\n')
        print(str)

    def export(self, host, user, password, dbname, table_name, outputpath):
        conn = pymysql.connect(host, user, password, dbname, charset='utf8')
        cursor = conn.cursor()

        count = cursor.execute('select * from ' + table_name)
        print(count)
        # 重置游标的位置
        cursor.scroll(0, mode='absolute')
        # 搜取所有结果
        results = cursor.fetchall()

        list = []
        for x in results:
            # print(x[0])
            list.append(x[0])

        # 获取MYSQL里面的数据字段名称
        fields = cursor.description
        workbook = xlwt.Workbook()
        sheet = workbook.add_sheet('table_' + table_name, cell_overwrite_ok=True)

        # 写上字段信息
        for field in range(0, len(fields)):
            sheet.write(0, field, fields[field][0])

        # 获取并写入数据段信息
        row = 1
        col = 0
        for row in range(1, len(results) + 1):
            for col in range(0, len(fields)):
                sheet.write(row, col, u'%s' % results[row - 1][col])

        workbook.save(outputpath)
        cursor.close()

    def window2(self):
        root = Tk()
        root.geometry("915x800")
        if self.targetid.replace("\n", "") in self.cheatlist:
            sql = "SELECT * FROM dhtdatabase.routing_list1 where fromid = '%s'" % self.targetid
        else:
            sql = "SELECT * FROM dhtdatabase.routing_list where fromid = '%s'" % self.rebuildid.hex()
        print(sql)
        result = self.database.selectRoutingList(sql)

        list = []
        for x in result:
            # print(x[0])
            list.append(x)

        s1 = Scrollbar(root)
        s1.pack(side=RIGHT, fill=Y)
        text = Text(root, width=130, height=61, yscrollcommand=s1.set)
        i = 0
        result_list = []
        count = 0
        while i < 160:
            j = 0
            for item in list:
                if (((int(item[0], 16) ^ int(item[3], 16))) >= pow(2, i)) and (
                        ((int(item[0], 16) ^ int(item[3], 16))) < pow(2, i + 1)):
                    # print(item)
                    result_list.append(item)
                    str1 = '{0:>20}'.format(str(item[1]))
                    str2 = '{0:>10}'.format(str(item[2]))
                    str3 = '{0:>50}'.format(str(item[3]))
                    stra = str(item[0]) + str1 + str2 + str3
                    text.insert(INSERT, stra + "\n")
                    count += 1
                    j += 1

            string = "2 ^ %d <= distance < 2 ^ %d" % (i, i + 1)
            text.insert(INSERT, string + "\n")
            text.insert(INSERT,
                        "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++" + "\n")
            i = i + 1
        print("count = " + str(count))
        # self.data(result_list)

        text.pack()

        s1.config(command=text.yview)
        # self.export('localhost', 'root', '123456', 'dhtdatabase', 'routing_list1', r'datetest.xls')
        root.mainloop()


if __name__ == "__main__":
    # max_node_qsize bigger, bandwith bigger, speed higher
    rebuildtool = RebuildTool("0.0.0.0", 6888, max_node_qsize=10000)
    thread1 = Thread(target=rebuildtool.recv_loop)
    thread2 = Thread(target=rebuildtool.send_loop)
    thread3 = Thread(target=rebuildtool.startPage_loop)
    thread4 = Thread(target=rebuildtool.resultPage_loop)
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    rebuildtool.countgo = 0
    while True:
        if rebuildtool.countgo >= 2:
            rebuildtool.outflag = False
            rebuildtool.min_distance = -1
            thread5 = Thread(target=rebuildtool.rebuild_routing_list)
            thread6 = Thread(target=rebuildtool.recv_loop)
            thread7 = Thread(target=rebuildtool.resultPage_loop)
            thread5.start()
            thread6.start()
            thread7.start()
            rebuildtool.countgo -= 1
        sleep(1)
