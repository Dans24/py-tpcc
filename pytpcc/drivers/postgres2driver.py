# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Adapted by Daniel Costa.
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
        "insertDeliveryEvent": "INSERT INTO delivery VALUES (%s, %s, %s)", # dl_delivery_d, dl_w_id, dl_carrier_id
        "getNewOrderDelivery": "SELECT o_d_id AS d_id, o_id AS no_o_id FROM orders_delivery_match WHERE dl_delivery_d = %s AND dl_w_id = %s", # ol_delivery_d, w_id
    },
    "NEW_ORDER": {
        "getWarehouseTaxRate": "SELECT w_tax FROM warehouse_view WHERE w_id = %s", # w_id
        "getDistrict": "SELECT d_tax, d_next_o_id FROM district_view WHERE D_ID = %s AND d_w_id = %s", # d_id, w_id
        "getCustomer": "SELECT c_discount, c_last, c_credit FROM customer_view WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s", # w_id, d_id, c_id
        "insertNewOrderEvent": "INSERT INTO orders VALUES (%s, %s, %s, %s)", # o_entry_d, o_d_id, o_w_id, o_c_id
        "getDistInfo": "SELECT {} FROM stock_view WHERE s_i_id = %s AND s_w_id = %s", # d_id, ol_i_id, ol_supply_w_id
        "insertOrderLine": "INSERT INTO order_line VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", # ol_entry_d, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_dist_info
        "getItemInfo": "SELECT i_price, i_name, i_data FROM item WHERE i_id = %s", # ol_i_id
        "getStockInfo": "SELECT s_quantity, s_data FROM stock_view WHERE s_i_id = %s AND s_w_id = %s" # d_id, ol_i_id, ol_supply_w_id
    },

    "PAYMENT": {
        "getCustomerByCustomerId": "SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_discount, c_data FROM customer_view WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s", # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_discount, c_data FROM customer_view WHERE c_w_id = %s AND c_d_id = %s AND c_last = %s ORDER BY c_first", # w_id, d_id, c_last
        "insertPaymentEvent": "INSERT INTO history VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    }
}

REPORT_MODE = True


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

        self.output = bool(config["output"])
                    
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
        result = None
        
        ol_delivery_d = params["ol_delivery_d"]
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        self.cursor.execute(q["insertDeliveryEvent"], [ol_delivery_d, w_id, o_carrier_id])
        
        if self.output:
            self.cursor.execute(q["getNewOrderDelivery"], [ol_delivery_d, w_id])
            result = self.cursor.fetchall()

        self.conn.commit()
        return result

    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        q = TXN_QUERIES["NEW_ORDER"]
        result = None
        
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

        if self.output:
            self.cursor.execute(q["getWarehouseTaxRate"], [w_id])
            w_tax = self.cursor.fetchone()[0]
            
            self.cursor.execute(q["getDistrict"], [d_id, w_id])
            district_info = self.cursor.fetchone()
            d_tax = district_info[0]
            d_next_o_id = 1 + district_info[1]

            self.cursor.execute(q["getCustomer"], [w_id, d_id, c_id])
            customer_info = self.cursor.fetchone()
            c_discount = customer_info[0]
        
        ## ----------------
        ## Insert Order Information
        ## ----------------
        self.cursor.execute(q["insertNewOrderEvent"], [o_entry_d, d_id, w_id, c_id])

        ## ----------------
        ## Insert Order Item Information
        ## ----------------
        item_data = []
        for i in range(len(i_ids)):
            ol_number = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = i_qtys[i]

            self.cursor.execute(SQL(q["getDistInfo"]).format(Identifier('s_dist_%02d'%d_id)), [ol_i_id, ol_supply_w_id])
            distInfo = self.cursor.fetchone()
            ol_dist_info = distInfo[0]

            self.cursor.execute(q["insertOrderLine"], [o_entry_d, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_dist_info])
            if self.output:
                self.cursor.execute(q["getItemInfo"], [ol_i_id])
                itemInfo = self.cursor.fetchone()
                i_name = itemInfo[1]
                i_data = itemInfo[2]
                i_price = itemInfo[0]

                self.cursor.execute(q["getStockInfo"], [ol_i_id, ol_supply_w_id])
                stockInfo = self.cursor.fetchone()
                s_quantity = stockInfo[0]
                s_data = stockInfo[1]

                if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                    brand_generic = 'B'
                else:
                    brand_generic = 'G'

                ol_amount = ol_quantity * i_price

                item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
        ## FOR
        
        ## Commit!
        self.conn.commit()

        if self.output:
            total *= (1 - c_discount) * (1 + w_tax + d_tax)

            misc = [ (w_tax, d_tax, d_next_o_id, total) ]
            result = [ customer_info, misc, item_data ]
        
        return result

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
        result = []

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
