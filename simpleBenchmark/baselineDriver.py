import random
import time
from abstractDriver import AbstractDriver
import psycopg2

class baselineDriver(AbstractDriver):

    def __init__(self, config):
        self.host = config["host"]
        self.database = config["database"]

    def connect(self):
        self.con = psycopg2.connect(host=self.host, database=self.database)
        self.cur = self.con.cursor()
        self.size = 1000000

    def load(self):
        self.cur.execute("CREATE TABLE test (id INT NOT NULL, value INT);")
        self.cur.execute("CREATE UNIQUE INDEX test_idx ON test (id);")
        
        self.cur.execute("INSERT INTO test SELECT g, FLOOR(RANDOM() * 100) FROM generate_series(1, %s) g;" % self.size)
        self.con.commit()

    def update(self):
        id = random.randint(1, self.size)
        value = random.randint(1, self.size)
        self.cur.execute("UPDATE test SET value = %s WHERE id = %s" % (value, id))
        self.con.commit()
    
    def query(self):
        self.cur.execute("SELECT SUM(value) FROM test;")
        total = self.cur.fetchone()
        self.con.commit()
        return total

    def clear(self):
        self.cur.execute("DROP TABLE IF EXISTS test;")
        self.con.commit()

    def rollbak(self):
        return self.con.rollback()

    def close(self):
        return self.con.close()