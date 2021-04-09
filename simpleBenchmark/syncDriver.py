import random
import time
from abstractDriver import AbstractDriver
import psycopg2

class Connection:
    def __init__(self, con):
        self.con = con
        self.cur = con.cursor()

class syncDriver(AbstractDriver):

    def connect(self):
        self.local = Connection(psycopg2.connect(host='localhost', database='local'))
        self.remote = Connection(psycopg2.connect(host='localhost', database='test'))
        self.size = 1000000

    def load(self):
        self.remote.cur.execute("DROP TABLE IF EXISTS test;")
        self.remote.con.commit()
        self.remote.cur.execute("CREATE TABLE test (id INT NOT NULL, date TIMESTAMP NOT NULL, value INT);")
        self.remote.cur.execute("CREATE UNIQUE INDEX test_idx ON test (id, date DESC);")
        self.remote.cur.execute("CREATE INDEX test_date_idx ON test (date DESC);")

        self.remote.cur.execute("INSERT INTO test SELECT g, NOW(), FLOOR(RANDOM() * 100) FROM generate_series(1, %s) g;" % self.size)
        
        #for i in range(1, self.size + 1):
        #    value = random.randint(1, self.size)
        #    self.remote.cur.execute("INSERT INTO test VALUES (%s, NOW(), %s);" % (i, value))
        self.remote.con.commit()

        self.local.cur.execute("DROP TABLE IF EXISTS test;")
        self.local.cur.execute("DROP TABLE IF EXISTS test_agg;")
        self.local.cur.execute("CREATE TABLE test (id INT NOT NULL, date TIMESTAMP NOT NULL, value INT);")
        self.local.cur.execute("CREATE UNIQUE INDEX local_test_idx ON test (id);")
        self.local.cur.execute("CREATE TABLE test_agg (col_name VARCHAR NOT NULL, agg_op VARCHAR NOT NULL, value BIGINT DEFAULT 0, last_update TIMESTAMP);")
        self.local.cur.execute("CREATE UNIQUE INDEX local_test_agg_idx ON test_agg (col_name, agg_op);")
        self.local.cur.execute("INSERT INTO test_agg VALUES ('%s', '%s');" % ("value", "sum"))
        self.local.con.commit()

    def update(self):
        id = random.randint(1, self.size)
        value = random.randint(1, self.size)
        self.remote.cur.execute("INSERT INTO test VALUES (%s, NOW(), %s);" % (id, value))
        self.remote.con.commit()
    
    def query(self):
        self.local.cur.execute("SELECT value, last_update FROM test_agg WHERE col_name = '%s' AND agg_op = '%s';" % ("value", "sum"))
        agg_value, last_update, = next(self.local.cur, (None, None))
        if last_update is None:
            self.remote.cur.execute("SELECT DISTINCT ON(id) id, value, date FROM test ORDER BY id, date DESC;")
        else:
            self.remote.cur.execute("SELECT DISTINCT ON(id) id, value, date FROM test WHERE date > '%s' ORDER BY id, date DESC;" % (last_update, ))
        for id, value, date in self.remote.cur:
            self.local.cur.execute("SELECT value FROM test WHERE id = %s" % (id, ))
            old_value = next(self.local.cur, 0)
            if old_value is None:
                self.local.cur.execute("INSERT INTO test VALUES (%s, '%s', '%s');" % (id, date, value))
            else:
                agg_value -= old_value
                self.local.cur.execute("UPDATE test SET date = '%s', value = '%s' WHERE id = %s;" % (date, value, id))
            agg_value += value
            last_update = max(last_update, date) if last_update is not None else date
        self.local.cur.execute("UPDATE test_agg SET last_update = '%s' WHERE col_name = '%s' AND agg_op = '%s';" % (last_update, "value", "sum"))
        self.local.con.commit()
        return agg_value

    def clear(self):
        self.remote.cur.execute("DROP TABLE test;")
        self.remote.con.commit()
        self.local.cur.execute("DROP TABLE test;")
        self.local.cur.execute("DROP TABLE metadata;")
        self.local.con.commit()

    def rollbak(self):
        print("rollback")
        self.remote.con.rollback()
        self.local.con.rollback()

    def close(self):
        self.remote.con.close()
        self.local.con.close()