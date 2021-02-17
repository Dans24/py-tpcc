# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Adapted to IoT by Daniel Costa.
# Adapted to PostgreSQL by jop.
#
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Original Java Version:
# Copyright (C) 2008
# Evan Jones
# Massachusetts Institute of Technology
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

from __future__ import with_statement

import os
import psycopg2
from psycopg2.sql import SQL, Identifier
import logging
import commands
from pprint import pprint,pformat

import constants
from abstractdriver import *

TXN_QUERIES = {
    "DELIVERY": {
        "insertDeliveryEvent": "INSERT INTO DELIVERY VALUES (%s, %s, %s)" # dl_delivery_d, dl_w_id, dl_carrier_id
    },
    "NEW_ORDER": {
        "insertOrderEvent": "INSERT INTO ORDERS VALUES (%s, %s, %s, %s)", # o_entry_d, o_d_id, o_w_id, o_c_id
        "insertOrderLine": "INSERT INTO ORDER_LINE VALUES (%s, %s, %s, %s, %s, %s, %s, %s)" # ol_entry_d, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_dist_info
    },

    "PAYMENT": {
        "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_DISCOUNT, C_DATA FROM CUSTOMER WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_DISCOUNT, C_DATA FROM CUSTOMER WHERE C_W_ID = %s AND C_D_ID = %s AND C_LAST = %s ORDER BY C_FIRST", # w_id, d_id, c_last
        "insertPaymentEvent": "INSERT INTO HISTORY VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
    }
}


## ==============================================
## PostgresDriver
## ==============================================
class PostgresDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "database": ("The connection string to the PostgreSQL database", "host=localhost dbname=tpcc" ),
        "schema": ("The schema in PostgreSQL database", "public" ),
    }
    
    def __init__(self, ddl):
        super(PostgresDriver, self).__init__("postgres", ddl)
        self.database = None
        self.conn = None
        self.cursor = None
    
    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return PostgresDriver.DEFAULT_CONFIG
    
    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        for key in PostgresDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)
        
        self.database = str(config["database"])
        self.schema = config["schema"]

        self.reset = bool(config["reset"])
                    
        self.conn = psycopg2.connect(self.database)
        self.cursor = self.conn.cursor()
        self.cursor.execute("SET search_path TO %s"%self.schema)

    ## ----------------------------------------------
    ## loadStart
    ## ----------------------------------------------
    def loadStart(self):
        if self.reset:
            logging.debug("Deleting database '%s'" % self.database)
            self.cursor.execute("DROP SCHEMA IF EXISTS %s CASCADE"%self.schema)
            self.cursor.execute("CREATE SCHEMA %s"%self.schema)
            self.cursor.execute("DROP DOMAIN IF EXISTS TINYINT")
            self.conn.commit()

        self.cursor.execute("select * from information_schema.tables where table_name=%s", ('order_line',))
        if self.cursor.rowcount <= 0:
            logging.debug("Loading DDL file '%s'" % (self.ddl))
            self.cursor.execute("CREATE DOMAIN TINYINT AS SMALLINT")
            self.cursor.execute(open(self.ddl, "r").read())
            self.conn.commit()

    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0: return
        
        p = ["%s"]*len(tuples[0])
        sql = "INSERT INTO %s VALUES (%s)" % (tableName, ",".join(p))
        self.cursor.executemany(sql, tuples)
        self.conn.commit()

        logging.debug("Loaded %d tuples for tableName %s" % (len(tuples), tableName))
        return

    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    def loadFinish(self):
        logging.info("Commiting changes to database")
        self.conn.commit()

    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):
        q = TXN_QUERIES["DELIVERY"]
        
        ol_delivery_d = params["ol_delivery_d"]
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]

        self.cursor.execute(q["insertDeliveryEvent"], [ol_delivery_d, w_id, o_carrier_id])
        # c_balance = c_balance + SUM(h_amount) - SUM(ol_amount)

        self.conn.commit()
        return True

    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        q = TXN_QUERIES["NEW_ORDER"]
        
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]
        i_dist_info = params["i_dist_info"]
            
        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)
        
        ## ----------------
        ## Insert Order Information
        ## ----------------
        self.cursor.execute(q["insertOrderEvent"], [o_entry_d, d_id, w_id, c_id])

        ## ----------------
        ## Insert Order Item Information
        ## ----------------
        for i in range(len(i_ids)):
            ol_number = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = i_qtys[i]
            ol_dist_info = i_dist_info[i]

            self.cursor.execute(q["insertOrderLine"], [o_entry_d, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_dist_info])
        ## FOR
        
        ## Commit!
        self.conn.commit()
        
        return True

    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        return False
        
    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------    
    def doPayment(self, params):
        q = TXN_QUERIES["PAYMENT"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]
        h_data = params["h_data"]

        if c_id != None:
            self.cursor.execute(q["getCustomerByCustomerId"], [w_id, d_id, c_id])
            customer = self.cursor.fetchone()
        else:
            # Get the midpoint customer's id
            self.cursor.execute(q["getCustomersByLastName"], [w_id, d_id, c_last])
            all_customers = self.cursor.fetchall()
            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = (namecnt-1)/2
            customer = all_customers[index]
            c_id = customer[0]
        assert len(customer) > 0
        # c_balance = SUM(h_amount) + SUM(dl_amount)
        # c_ytd_payment = SUM(h_amount)
        # c_payment_cnt = COUNT(*)

        # Create the history record
        self.cursor.execute(q["insertPaymentEvent"], [c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_data])

        self.conn.commit()

        return True
        
    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------    
    def doStockLevel(self, params):
        return False
        
## CLASS
