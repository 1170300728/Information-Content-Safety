import pymysql


class mydatabase():
    def __init__(self):
        self.conn = pymysql.connect(
            host='localhost',
            port=3306,
            user='root',
            password='123456',
            db='dhtdatabase',
            charset='utf8'
        )

    def myselect(self, select_id):
        cursor = self.conn.cursor()
        sql = 'select count(*) from dht_node where node_id = "%s"' % select_id.hex()
        cursor.execute(sql)
        if (cursor.fetchone()[0] == 0):
            print('node with id = %s NOT FOUND' % select_id.hex())
            return None
        else:
            sql = 'select * from dht_node where node_id = "%s"' % select_id.hex()
            cursor.execute(sql)
            node = tuple(cursor.fetchone())
            cursor.close()
            return node

    def delete_routinglist(self, id):
        cursor = self.conn.cursor()
        sql = "delete from routing_list where fromid = '%s'" % id
        cursor.execute(sql)
        self.conn.commit()
        cursor.close()

    def select_specific_dhtnode(self, id):
        cursor = self.conn.cursor()
        sql = "select * from dht_node where node_id = '%s'" % id
        cursor.execute(sql)
        result = cursor.fetchone()
        cursor.close()
        return result

    def select_ch_dht(self):
        list = []
        cursor = self.conn.cursor()
        sql = 'select * from dht_node '
        cursor.execute(sql)
        for i in range(105):
            list.append(cursor.fetchone())
        cursor.close()
        return list

    def selectDHTNode(self):
        cursor = self.conn.cursor()
        sql = 'select node_id from dht_node '
        cursor.execute(sql)
        result = cursor.fetchall()
        list = []
        for x in result:
            print(x[0])
            list.append(x[0])
        cursor.close()
        return list

    def selectRoutingList(self, sql):
        cursor = self.conn.cursor()
        # sql = 'SELECT distinct fromid FROM dhtdatabase.routing_list'
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result

    def myinsert(self, insert_id, insert_ip, insert_port):
        cursor = self.conn.cursor()
        print("insert_id=" + str(insert_id.hex()))
        sql = 'select count(*) from dht_node where node_id = "%s"' % insert_id.hex()
        cursor.execute(sql)
        if (cursor.fetchone()[0] == 0):
            sql = 'insert into dht_node(node_id, node_ip, node_port) values("%s", "%s", "%s")' % (
            insert_id.hex(), insert_ip, str(insert_port))
            cursor.execute(sql)
            self.conn.commit()
            cursor.close()
            return True
        else:
            print("node with id = %s FOUND, don't insert" % insert_id.hex())
            return False

    def myinsert2(self, list):
        cursor = self.conn.cursor()
        sql0 = 'DROP TABLE `dhtdatabase`.`routing_list1`;'
        sql = 'CREATE TABLE `routing_list1` (`nodeid` varchar(45) NOT NULL,`nodeip` varchar(45) NOT NULL,`nodeport` int(11) NOT NULL,`fromid` varchar(45) NOT NULL,PRIMARY KEY (`nodeid`),UNIQUE KEY `nodeid_UNIQUE` (`nodeid`)) ENGINE=InnoDB DEFAULT CHARSET=latin1'
        # cursor.execute(sql0)
        # cursor.execute(sql)
        for item in list:
            sql = 'insert into routing_list(nodeid, nodeip, nodeport, fromid) values("%s", "%s", "%s","%s")' % (
            item[0], item[1], item[2], item[3])
            cursor.execute(sql)
        self.conn.commit()
        cursor.close()

    def myinsert_rebuild(self, insert_id, insert_ip, insert_port, from_id):
        cursor = self.conn.cursor()
        print("myinsert_rebuild" + str(insert_id.hex()))
        sql = 'select count(*) from routing_list where nodeid = "%s"' % insert_id.hex()

        cursor.execute(sql)
        if (cursor.fetchone()[0] == 0):
            sql = 'insert into routing_list(nodeid, nodeip, nodeport, fromid) values("%s", "%s", "%s" ,"%s")' % (
            insert_id.hex(), insert_ip, str(insert_port), from_id.hex())
            cursor.execute(sql)
            self.conn.commit()
            cursor.close()
            return True
        else:
            print("node with id = %s FOUND, don't insert" % insert_id.hex())
            return False

    def myclose(self):
        self.conn.close()
