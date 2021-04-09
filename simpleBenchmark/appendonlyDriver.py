import random
import time
from abstractDriver import AbstractDriver
import psycopg2

class appendonlyDriver(AbstractDriver):

    def connect(self):
        self.con = psycopg2.connect(host='localhost', database='test')
        self.cur = self.con.cursor()
        self.size = 1000000

    def load(self):
        self.cur.execute("DROP TABLE IF EXISTS test;")
        self.cur.execute("CREATE TABLE test (id INT NOT NULL, date TIMESTAMP NOT NULL, value INT);")
        self.cur.execute("CREATE UNIQUE INDEX test_idx ON test (id, date DESC);")
        
        self.cur.execute("INSERT INTO test SELECT g, NOW(), FLOOR(RANDOM() * 100) FROM generate_series(1, %s) g;" % self.size)

        #for i in range(1, self.size + 1):
        #    self.cur.execute("INSERT INTO test VALUES (%s, NOW(), %s);" % (i, random.randint(1, self.size)))
        #self.con.commit()

    def update(self):
        id = random.randint(1, self.size)
        value = random.randint(1, self.size)
        self.cur.execute("INSERT INTO test VALUES (%s, NOW(), %s);" % (id, value))
        self.con.commit()
    
    def query(self):
        self.cur.execute("SELECT SUM(value) FROM (SELECT DISTINCT ON(id) value FROM test ORDER BY id, date DESC) values;")
        total = self.cur.fetchone()
        self.con.commit()
        return total

    def clear(self):
        self.cur.execute("DROP TABLE test;")
        self.con.commit()

    def rollbak(self):
        return self.con.rollback()

    def close(self):
        return self.con.close()