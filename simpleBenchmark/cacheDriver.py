import random
import time
from abstractDriver import AbstractDriver
import psycopg2

class Connection:
    def __init__(self, con):
        self.con = con
        self.cur = con.cursor()

class cacheDriver(AbstractDriver):

    def __init__(self, config):
        self.local = {
            "host": config["host"]
            "database": config["database"]
        }
        self.size = config["size"]

    def connect(self):
        self.local = Connection(psycopg2.connect(host='localhost', database='local'))
        self.remote = Connection(psycopg2.connect(host='104.155.108.143', database='postgres', user='postgres', password='O7uAwJleoDGl6jxB'))
        self.size = 1000000

    def load(self):
        self.remote.cur.execute("DROP TABLE IF EXISTS test;")
        self.remote.con.commit()
        self.remote.cur.execute("CREATE TABLE test (id INT NOT NULL, date TIMESTAMP NOT NULL, value INT);")
        self.remote.cur.execute("CREATE UNIQUE INDEX test_idx ON test (id, date DESC);")

        self.remote.cur.execute("INSERT INTO test SELECT g, NOW(), FLOOR(RANDOM() * 100) FROM generate_series(1, %s) g;" % self.size)
        
        #for i in range(1, self.size + 1):
        #    value = random.randint(1, self.size)
        #    self.remote.cur.execute("INSERT INTO test VALUES (%s, NOW(), %s);" % (i, value))
        self.remote.con.commit()

        self.local.cur.execute("DROP TABLE IF EXISTS cache;")
        self.local.cur.execute("""
            CREATE TABLE id_col (
                stmt VARCHAR,
                id_col VARCHAR,
                PRIMARY KEY(stmt, id_col)
            )
        """)
        self.local.cur.execute("""
            CREATE TABLE ord_col (
                stmt VARCHAR,
                ord_col VARCHAR,
                PRIMARY KEY(stmt, ord_col)
            )
        """)
        self.local.cur.execute("""
            CREATE TABLE cache (
                stmt VARCHAR,
                col_name VARCHAR,
                agg_op VARCHAR
                id INT,
                value INT,
                last_update TIMESTAMP,
                PRIMARY KEY (stmt, col_name, agg_op, id)
            )
        """)
        self.local.con.commit()

    def update(self):
        id = random.randint(1, self.size)
        value = random.randint(1, self.size)
        self.remote.cur.execute("INSERT INTO test VALUES (%s, NOW(), %s);" % (id, value))
        self.remote.con.commit()
    
    def query(self):
        self.local.cur.execute("SELECT value, last_update FROM cache WHERE stmt = '%s' AND column_name = '%s' AND agg_op = '%s';" % ("SELECT DISTINCT ON(id) * FROM test ORDER BY id, date DESC", "value", "sum"))
        value, timestamp = next(self.local.cur, (None, None))
        if timestamp is None:
            self.remote.cur.execute("SELECT DISTINCT ON(id) id, value, date FROM test ORDER BY id, date DESC;")
        else:
            self.remote.cur.execute("SELECT DISTINCT ON(id) id, value, date FROM test WHERE date > '%s' ORDER BY id, date DESC;" % (timestamp, ))
        for id, value, date in self.remote.cur:
            self.local.cur.execute("INSERT INTO test VALUES (%s, '%s', '%s') ON CONFLICT (id) DO UPDATE SET date = '%s', value = '%s';" % (id, date, value, date, value))
        self.local.cur.execute("SELECT SUM(value), MAX(date) FROM test;")
        total, new_timestamp = next(self.local.cur, (None, None))
        self.local.cur.execute("UPDATE metadata SET last_date = %s WHERE table_name = %s;", (new_timestamp, "test"))
        self.remote.con.commit()
        self.local.con.commit()
        return total

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

    def getFromCache(self, stmt, col_name, agg_op):
        "SELECT DISTINCT ON(id) * FROM test ORDER BY id, date DESC"
        self.local.cur.execute("SELECT value, last_update FROM cache WHERE stmt = '%s' AND col_name = '%s' AND agg_op = '%s';" % (stmt, col_name, agg_op))
        value, last_update = next(self.local.cur, (None, None))
        self.local.cur.execute("SELECT id_col FROM id_col WHERE stmt = '%s'" % (stmt))
        id_cols = [col for col, in self.local.cur]
        self.local.cur.execute("SELECT ord_col FROM ord_col WHERE stmt = '%s'" % (stmt))
        ord_cols = [col for col, in self.local.cur]
        cols = [col_name] + id_cols + ord_cols
        if last_update is None:
            self.remote.cur.execute("SELECT DISTINCT ON(id) %s FROM test ORDER BY id, date DESC;" % (", ".join(cols)))
        else:
            self.remote.cur.execute("SELECT DISTINCT ON(id) %s FROM test WHERE date > '%s' ORDER BY id, date DESC;" % (", ".join(cols), last_update ))
        for id, value, date in self.remote.cur:
            self.local.cur.execute("INSERT INTO test VALUES (%s, '%s', '%s') ON CONFLICT (id) DO UPDATE SET date = '%s', value = '%s';" % (id, date, value, date, value))
        self.local.cur.execute("SELECT SUM(value), MAX(date) FROM test;")
        total, new_timestamp = next(self.local.cur, (None, None))
        self.local.cur.execute("UPDATE metadata SET last_date = %s WHERE table_name = %s;", (new_timestamp, "test"))
        self.remote.con.commit()
        self.local.con.commit()
        return total