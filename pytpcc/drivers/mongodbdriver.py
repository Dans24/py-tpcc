# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
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
import sys
import logging
import pymongo
from pprint import pprint, pformat

import constants
from abstractdriver import *

TABLE_COLUMNS = {
    constants.TABLENAME_ITEM: [
        "i_id",  # INTEGER
        "i_im_id",  # INTEGER
        "i_name",  # VARCHAR
        "i_price",  # FLOAT
        "i_data",  # VARCHAR
    ],
    constants.TABLENAME_WAREHOUSE: [
        "w_id",  # SMALLINT
        "w_name",  # VARCHAR
        "w_street_1",  # VARCHAR
        "w_street_2",  # VARCHAR
        "w_city",  # VARCHAR
        "w_state",  # VARCHAR
        "w_zip",  # VARCHAR
        "w_tax",  # FLOAT
    ],
    constants.TABLENAME_DISTRICT: [
        "d_id",  # TINYINT
        "d_w_id",  # SMALLINT
        "d_name",  # VARCHAR
        "d_street_1",  # VARCHAR
        "d_street_2",  # VARCHAR
        "d_city",  # VARCHAR
        "d_state",  # VARCHAR
        "d_zip",  # VARCHAR
        "d_tax",  # FLOAT
    ],
    constants.TABLENAME_CUSTOMER:   [
        "c_id",  # INTEGER
        "c_d_id",  # TINYINT
        "c_w_id",  # SMALLINT
        "c_first",  # VARCHAR
        "c_middle",  # VARCHAR
        "c_last",  # VARCHAR
        "c_street_1",  # VARCHAR
        "c_street_2",  # VARCHAR
        "c_city",  # VARCHAR
        "c_state",  # VARCHAR
        "c_zip",  # VARCHAR
        "c_phone",  # VARCHAR
        "c_since",  # TIMESTAMP
        "c_credit",  # VARCHAR
        "c_credit_lim",  # FLOAT
        "c_discount",  # FLOAT
        "c_data",  # VARCHAR
    ],
    constants.TABLENAME_STOCK:      [
        "s_i_id",  # INTEGER
        "s_w_id",  # SMALLINT
        "s_dist_01",  # VARCHAR
        "s_dist_02",  # VARCHAR
        "s_dist_03",  # VARCHAR
        "s_dist_04",  # VARCHAR
        "s_dist_05",  # VARCHAR
        "s_dist_06",  # VARCHAR
        "s_dist_07",  # VARCHAR
        "s_dist_08",  # VARCHAR
        "s_dist_09",  # VARCHAR
        "s_dist_10",  # VARCHAR
        "s_data",  # VARCHAR
    ],
    constants.TABLENAME_ORDERS:     [
        "o_id",  # INTEGER
        "o_d_id",  # TINYINT
        "o_w_id",  # SMALLINT
        "o_c_id",  # INTEGER
        "o_entry_d",  # TIMESTAMP
    ],
    constants.TABLENAME_ORDER_LINE: [
        "ol_o_id",  # INTEGER
        "ol_d_id",  # TINYINT
        "ol_w_id",  # SMALLINT
        "ol_number",  # INTEGER
        "ol_i_id",  # INTEGER
        "ol_supply_w_id",  # SMALLINT
        "ol_quantity",  # INTEGER
        "ol_dist_info",  # VARCHAR
    ],
    constants.TABLENAME_DELIVERY:  [
        "dl_delivery_d",  # TIMESTAMP
        "dl_w_id",  # TINYINT
        "dl_carrier_id",  # SMALLINT
    ],
    constants.TABLENAME_DELIVERY_ORDERS:  [
        "dlo_delivery_d",  # INTEGER
        "dlo_w_id",  # TINYINT
        "dlo_o_id",  # INT
        "dlo_d_id",  # SMALLINT
    ],
    constants.TABLENAME_HISTORY:    [
        "h_c_id",  # INTEGER
        "h_c_d_id",  # TINYINT
        "h_c_w_id",  # SMALLINT
        "h_d_id",  # TINYINT
        "h_w_id",  # SMALLINT
        "h_date",  # TIMESTAMP
        "h_amount",  # FLOAT
        "h_data",  # VARCHAR
    ],
}
TABLE_INDEXES = {
    constants.TABLENAME_ITEM: [
        "i_id",
    ],
    constants.TABLENAME_WAREHOUSE: [
        "w_id",
    ],
    constants.TABLENAME_DISTRICT: [
        [("d_w_id", pymongo.ASCENDING), ("d_id", pymongo.ASCENDING)]
    ],
    constants.TABLENAME_CUSTOMER:   [
        [("c_w_id", pymongo.ASCENDING), ("c_d_id",
                                         pymongo.ASCENDING), ("c_id", pymongo.ASCENDING)],
        [("c_w_id", pymongo.ASCENDING), ("c_d_id",
                                         pymongo.ASCENDING), ("c_last", pymongo.ASCENDING)]
    ],
    constants.TABLENAME_ORDERS:   [
        [("o_w_id", pymongo.ASCENDING), ("o_d_id",
                                         pymongo.ASCENDING), ("o_id", pymongo.ASCENDING)],

        # Denormalized ORDER_LINE
        [("o_w_id", pymongo.ASCENDING), ("o_d_id", pymongo.ASCENDING), ("o_id", pymongo.ASCENDING),
         ("order_line.supply_w_id", pymongo.ASCENDING), ("order_line.ol_i_id", pymongo.ASCENDING)],
    ],
    constants.TABLENAME_STOCK:      [
        [("s_w_id", pymongo.ASCENDING), ("s_i_id", pymongo.ASCENDING)]
    ],
    constants.TABLENAME_DELIVERY:   [
        [("dl_w_id", pymongo.ASCENDING), ("delivery.dlo_d_id",
                                          pymongo.ASCENDING), ("dlo_w_id", pymongo.ASCENDING)]
    ]
}

DENORMALIZED_TABLES = {
    (constants.TABLENAME_ORDERS, 3): [constants.TABLENAME_ORDER_LINE],
    (constants.TABLENAME_DELIVERY, 2): [constants.TABLENAME_DELIVERY_ORDERS],
}

PIPELINES = {
    "stock_stats": ("orders",
[
  {
    "$unwind":"$order_line"
  },
  {
    "$group":{
      "_id":[
        "$order_line.ol_supply_w_id",
        "$order_line.ol_i_id"
      ],
      "s_ytd":{
        "$sum":"order_line.ol_amount"
      },
      "s_order_cnt":{
        "$sum":1
      },
      "s_remote_cnt":{
        "$sum":{
          "$cond":{
            "if":{
              "$ne":[
                "$order_line.ol_supply_w_id",
                "o_w_id"
              ]
            },
            "then":0,
            "else":1
          }
        }
      },
      "total_quantity":{
        "$sum":"order_line.ol_quantity"
      }
    }
  },
  {
    "$project":{
      "s_quantity":{
        "$add":[
          {"$mod":[
              {"$add":[
                  {"$mod":[
                    {"$subtract":[0,"$total_quantity"]}, 91]
                  }, 91]
              }, 91]
          }, 10]
      }
    }
  }
])
}

# ==============================================
# MongodbDriver
# ==============================================


class MongodbDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "uri":          ("The connection URI to mongod", "mongodb://localhost:27017"),
        "name":         ("Collection name", "tpcc"),
    }

    def __init__(self, ddl):
        super(MongodbDriver, self).__init__("mongodb", ddl)
        self.database = None
        self.conn = None

        # Create member mapping to collections
        for name in constants.ALL_TABLES:
            self.__dict__[name.lower()] = None

    # ----------------------------------------------
    # makeDefaultConfig
    # ----------------------------------------------
    def makeDefaultConfig(self):
        return MongodbDriver.DEFAULT_CONFIG

    # ----------------------------------------------
    # loadConfig
    # ----------------------------------------------
    def loadConfig(self, config):
        for key in MongodbDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (
                key, self.name)

        self.conn = pymongo.MongoClient(config['uri'])
        self.database = self.conn[str(config['name'])]
        self.denormalized_values = {}

        if config["reset"]:
            logging.debug("Deleting database '%s'" % self.database.name)
            for name in constants.ALL_TABLES:
                if name in self.database.collection_names():
                    self.database.drop_collection(name)
                    logging.debug("Dropped collection %s" % name)
        # IF

        # Setup!
        load_indexes = ('execute' in config and not config['execute']) and \
                       ('load' in config and not config['load'])
        for name in constants.ALL_TABLES:
            if name in DENORMALIZED_TABLES.keys():
                continue

            # Create member mapping to collections
            self.__dict__[name] = self.database[name]

            # Create Indexes
            if load_indexes and name in TABLE_INDEXES:
                logging.debug("Creating index for %s" % name)
                for index in TABLE_INDEXES[name]:
                    self.database[name].create_index(index)
        # FOR

    # ----------------------------------------------
    # loadTuples
    # ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0:
            return
        logging.debug("Loading %d tuples for tableName %s" %
                      (len(tuples), tableName))

        assert tableName in TABLE_COLUMNS, "Unexpected table %s" % tableName
        columns = TABLE_COLUMNS[tableName]
        num_columns = range(len(columns))

        tuple_dicts = []

        if tableName in sum(DENORMALIZED_TABLES.values(), []):
            parentTable, splitIdx = next(
                k for k, v in DENORMALIZED_TABLES.items() if tableName in v)
            print(tableName + " -> " + parentTable)
            parentDict = self.denormalized_values[parentTable]
            for t in tuples:
                key = tuple(t[:splitIdx])
                value = t[splitIdx:]
                values = parentDict[key].get(tableName, [])
                values.append(
                    dict(map(lambda i: (columns[i], t[i]), num_columns[splitIdx:])))
                parentDict[key][tableName] = values
            # FOR

        elif tableName in map(lambda t: t[0], DENORMALIZED_TABLES.keys()):
            print(tableName)
            splitIdx = next(
                i for t, i in DENORMALIZED_TABLES.keys() if t == tableName)
            self.denormalized_values[tableName] = self.denormalized_values.get(tableName, {})
            for t in tuples:
                key = tuple(t[:splitIdx])
                self.denormalized_values[tableName][key] = dict(
                    map(lambda i: (columns[i], t[i]), num_columns))
        # Otherwise just shove the tuples straight to the target collection
        else:
            for t in tuples:
                tuple_dicts.append(
                    dict(map(lambda i: (columns[i], t[i]), num_columns)))
            # FOR
            self.database[tableName].insert(tuple_dicts)
        # IF

        return

    # ----------------------------------------------
    # loadFinishDistrict
    # ----------------------------------------------
    def loadFinishDistrict(self, w_id, d_id):
        pass

    # ----------------------------------------------
    # loadFinish
    # ----------------------------------------------
    def loadFinish(self):
        for parentName, _ in DENORMALIZED_TABLES.keys():
            tuple_dict = self.denormalized_values[parentName].values()

            logging.debug("Pushing %d denormalized %s records into MongoDB" % (
                len(tuple_dict), parentName))
        
            self.database[parentName].insert_many(tuple_dict)
        self.denormalized_values.clear()

    def aggregate(self, pipeline, query, projection, one=False):
        collection_name, pipeline = PIPELINES[pipeline]
        agg = self.database[collection_name].aggregate(
            [{"$match": query}] +
            pipeline +
            [{"$project": projection}] +
            ([{"$limit": 1}] if one else [])
        )
        if one:
            return next(agg, None)
        return agg

    # ----------------------------------------------
    # doDelivery
    # ----------------------------------------------

    def doDelivery(self, params):
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]

        result = []
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
            # getNewOrder
            next_no_id = next(self.delivery
                .find({"dl_w_id": w_id, "delivery_orders.dlo_d_id": d_id},
                {"delivery_orders.dlo_o_id.$": 1 })
                .sort([("delivery_orders.dlo_o_id", -1)])
                .limit(1))["delivery_orders"][0]["dlo_o_id"] + 1 # MAX(dlo_o_id) + 1
            no = self.orders.find_one(
                {"o_d_id": d_id, "o_w_id": w_id, "o_id": next_no_id}, {"o_id": 1})
            if no == None:
                # No orders for this district: skip it. Note: This must be reported if > 1%
                continue
            assert len(no) > 0
            o_id = no["o_id"]

            result.append((d_id, o_id))
        # FOR
        self.delivery.insert({
            "dl_delivery_d": ol_delivery_d,
            "dl_w_id": w_id,
            "delivery_orders": [{"dlo_d_id": d_id, "dlo_o_id": o_id} for d_id, o_id in result]
        })
        return result

    # ----------------------------------------------
    # doNewOrder
    # ----------------------------------------------
    def doNewOrder(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]
        s_dist_col = "s_dist_%02d" % d_id

        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        items = self.item.find({"i_id": {"$in": i_ids}}, {
                               "i_id": 1, "i_price": 1, "i_name": 1, "i_data": 1})
        items = list(items)
        # TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
        # Note that this will happen with 1% of transactions on purpose.
        if len(items) != len(i_ids):
            # TODO Abort here!
            return
        # IF

        # ----------------
        # Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
        # ----------------

        # getWarehouseTaxRate
        w = self.warehouse.find_one({"w_id": w_id}, {"w_tax": 1})
        assert w
        w_tax = w["w_tax"]

        # getDistrict
        d = self.district.find_one({"d_id": d_id, "d_w_id": w_id}, {"d_tax": 1})
        assert d
        d_tax = d["d_tax"]
        d_next_o_id = next(self.orders
            .find({"o_d_id": d_id, "o_w_id": w_id}, {"o_id": 1})
            .sort([("o_id", -1)])
            .limit(1)
        )["o_id"] + 1 # MAX(o_id) + 1

        # getCustomer
        c = self.customer.find_one({"c_id": c_id, "c_d_id": d_id, "c_w_id": w_id}, {
                                   "c_discount": 1, "c_last": 1, "c_credit": 1})
        assert c
        c_discount = c["c_discount"]

        # createNewOrder
        o = {
            "o_id": d_next_o_id,
            "o_d_id": d_id,
            "o_w_id": w_id,
            "o_c_id": c_id,
            "o_entry_d": o_entry_d,
            constants.TABLENAME_ORDER_LINE: []
        }

        # ----------------
        # OPTIMIZATION:
        # If all of the items are at the same warehouse, then we'll issue a single
        # request to get their information
        # ----------------
        ol_cnt = len(i_ids)

        # ----------------
        # Insert Order Item Information
        # ----------------
        item_data = []
        total = 0
        for i in range(ol_cnt):
            ol_number = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = i_qtys[i]

            itemInfo = items[i]
            i_name = itemInfo["i_name"]
            i_data = itemInfo["i_data"]
            i_price = itemInfo["i_price"]

            # getStockInfo
            si = self.stock.find_one({"s_i_id": ol_i_id, "s_w_id": w_id}, {
                                         "s_i_id": 1, "s_quantity": 1, "s_data": 1, "s_ytd": 1, "s_order_cnt": 1, "s_remote_cnt": 1, s_dist_col: 1})
            
            ss = self.aggregate("stock_stats", {"s_i_id": ol_i_id, "s_w_id": w_id}, {
                            "s_quantity": 1, "s_ytd": 1, "s_order_cnt": 1, "s_remote_cnt": 1})
            ss = next(ss, {
                "s_quantity": 10,
                "s_ytd": 0,
                "s_order_cnt": 0,
                "s_remote_cnt": 0
            })
            assert si, "Failed to find s_i_id: %d\n%s" % (
                ol_i_id, pformat(itemInfo))

            s_quantity = ss["s_quantity"]
            s_ytd = ss["s_ytd"]
            s_order_cnt = ss["s_order_cnt"]
            s_remote_cnt = ss["s_remote_cnt"]
            s_data = si["s_data"]
            # Fetches data from the s_dist_[d_id] column
            s_dist_xx = si[s_dist_col]

            if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'
            # Transaction profile states to use "ol_quantity * i_price"
            ol_amount = ol_quantity * i_price
            total += ol_amount

            ol = {"ol_o_id": d_next_o_id, "ol_number": ol_number, "ol_i_id": ol_i_id, "ol_supply_w_id": ol_supply_w_id,
                  "ol_quantity": ol_quantity, "ol_amount": ol_amount, "ol_dist_info": s_dist_xx}

            
            
            o[constants.TABLENAME_ORDER_LINE].append(ol)

            # Add the info to be returned
            item_data.append(
                (i_name, s_quantity, brand_generic, i_price, ol_amount))
        # FOR

        self.orders.insert(o)
        # Adjust the total for the discount
        # print "c_discount:", c_discount, type(c_discount)
        # print "w_tax:", w_tax, type(w_tax)
        # print "d_tax:", d_tax, type(d_tax)
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        # Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [(w_tax, d_tax, d_next_o_id, total)]

        return [c, misc, item_data]

    # ----------------------------------------------
    # doOrderStatus
    # ----------------------------------------------
    def doOrderStatus(self, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]

        assert w_id, pformat(params)
        assert d_id, pformat(params)

        search_fields = {"c_w_id": w_id, "c_d_id": d_id}
        return_fields = {"c_id": 1, "c_first": 1,
                         "c_middle": 1, "c_last": 1, "c_balance": 1}
        if self.denormalize:
            for f in ['o_id', 'o_carrier_id', 'o_entry_d']:
                return_fields["%s.%s" % (constants.TABLENAME_ORDERS, f)] = 1
            for f in ['ol_supply_w_id', 'ol_i_id', 'ol_quantity']:
                return_fields["%s.%s.%s" % (
                    constants.TABLENAME_ORDERS, constants.TABLENAME_ORDER_LINE, f)] = 1
        # IF

        if c_id != None:
            # getCustomerByCustomerId
            search_fields["c_id"] = c_id
            c = self.customer.find_one(search_fields, return_fields)
            assert c

        else:
            # getCustomersByLastName
            # Get the midpoint customer's id
            search_fields['c_last'] = c_last

            all_customers = self.customer.find(search_fields, return_fields)
            namecnt = all_customers.count()
            assert namecnt > 0
            index = (namecnt-1)/2
            c = all_customers[index]
            c_id = c["c_id"]
        assert len(c) > 0
        assert c_id != None

        orderLines = []
        order = None

        if self.denormalize:
            # getLastOrder
            if constants.TABLENAME_ORDERS in c:
                order = c[constants.TABLENAME_ORDERS][-1]
                # getOrderLines
                orderLines = order[constants.TABLENAME_ORDER_LINE]
        else:
            # getLastOrder
            order = self.orders.find({"O_W_ID": w_id, "O_D_ID": d_id, "O_C_ID": c_id}, {
                                     "O_ID": 1, "O_CARRIER_ID": 1, "O_ENTRY_D": 1}).sort("O_ID", direction=pymongo.DESCENDING).limit(1)[0]
            o_id = order["O_ID"]

            if order:
                # getOrderLines
                orderLines = self.order_line.find({"OL_W_ID": w_id, "OL_D_ID": d_id, "OL_O_ID": o_id}, {
                                                  "OL_SUPPLY_W_ID": 1, "OL_I_ID": 1, "OL_QUANTITY": 1, "OL_AMOUNT": 1, "OL_DELIVERY_D": 1})
        # IF

        return [c, order, orderLines]

    # ----------------------------------------------
    # doPayment
    # ----------------------------------------------
    def doPayment(self, params):
        return
        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]

        search_fields = {"C_W_ID": w_id, "C_D_ID": d_id}
        return_fields = {"C_BALANCE": 0,
                         "C_YTD_PAYMENT": 0, "C_PAYMENT_CNT": 0}

        if c_id != None:
            # getCustomerByCustomerId
            search_fields["C_ID"] = c_id
            c = self.customer.find_one(search_fields, return_fields)
            assert c

        else:
            # getCustomersByLastName
            # Get the midpoint customer's id
            search_fields['C_LAST'] = c_last
            all_customers = self.customer.find(search_fields, return_fields)
            namecnt = all_customers.count()
            assert namecnt > 0
            index = (namecnt-1)/2
            c = all_customers[index]
            c_id = c["C_ID"]
        assert len(c) > 0
        assert c_id != None

        if c_id != None:
            # getCustomerByCustomerId
            c = self.customer.find_one(
                {"C_W_ID": w_id, "C_D_ID": d_id, "C_ID": c_id})
        else:
            # getCustomersByLastName
            # Get the midpoint customer's id
            all_customers = self.customer.find(
                {"C_W_ID": w_id, "C_D_ID": d_id, "C_LAST": c_last})
            namecnt = all_customers.count()
            assert namecnt > 0
            index = (namecnt-1)/2
            c = all_customers[index]
            c_id = c["C_ID"]
        assert len(c) > 0
        assert c_id != None
        c_data = c["C_DATA"]

        # getWarehouse
        w = self.warehouse.find_one({"W_ID": w_id}, {
                                    "W_NAME": 1, "W_STREET_1": 1, "W_STREET_2": 1, "W_CITY": 1, "W_STATE": 1, "W_ZIP": 1})
        assert w

        # updateWarehouseBalance
        self.warehouse.update({"_id": w["_id"]}, {"$inc": {"W_YTD": h_amount}})

        # getDistrict
        d = self.district.find_one({"D_W_ID": w_id, "D_ID": d_id}, {
                                   "D_NAME": 1, "D_STREET_1": 1, "D_STREET_2": 1, "D_CITY": 1, "D_STATE": 1, "D_ZIP": 1})
        assert d

        # updateDistrictBalance
        self.district.update({"_id": d["_id"]},  {"$inc": {"D_YTD": h_amount}})

        # Build CUSTOMER update command
        customer_update = {"$inc": {"C_BALANCE": h_amount*-
                                    1, "C_YTD_PAYMENT": h_amount, "C_PAYMENT_CNT": 1}}

        # Customer Credit Information
        if c["C_CREDIT"] == constants.BAD_CREDIT:
            newData = " ".join(
                map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA:
                c_data = c_data[:constants.MAX_C_DATA]
            customer_update["$set"] = {"C_DATA": c_data}
        # IF

        # Concatenate w_name, four spaces, d_name
        h_data = "%s    %s" % (w["W_NAME"], d["D_NAME"])
        h = {"H_D_ID": d_id, "H_W_ID": w_id, "H_DATE": h_date,
             "H_AMOUNT": h_amount, "H_DATA": h_data}
        if self.denormalize:
            # insertHistory + updateCustomer
            customer_update["$push"] = {constants.TABLENAME_HISTORY: h}
            self.customer.update({"_id": c["_id"]}, customer_update)
        else:
            # updateCustomer
            self.customer.update({"_id": c["_id"]}, customer_update)

            # insertHistory
            self.history.insert(h)

        # TPC-C 2.5.3.3: Must display the following fields:
        # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
        # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
        # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
        # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
        # H_AMOUNT, and H_DATE.

        # Hand back all the warehouse, district, and customer data
        return [w, d, c]

    # ----------------------------------------------
    # doStockLevel
    # ----------------------------------------------
    def doStockLevel(self, params):
        return
        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]

        # getOId
        d = self.district.find_one(
            {"D_W_ID": w_id, "D_ID": d_id}, {"D_NEXT_O_ID": 1})
        assert d
        o_id = d["D_NEXT_O_ID"]

        # getStockCount
        # Outer Table: ORDER_LINE
        # Inner Table: STOCK
        if self.denormalize:
            c = self.customer.find({"C_W_ID": w_id, "C_D_ID": d_id, "ORDERS.O_ID": {
                                   "$lt": o_id, "$gte": o_id-20}}, {"ORDERS.ORDER_LINE.OL_I_ID": 1})
            assert c
            orderLines = []
            for ol in c:
                assert "ORDER_LINE" in ol["ORDERS"][0]
                orderLines.extend(ol["ORDERS"][0]["ORDER_LINE"])
        else:
            orderLines = self.order_line.find({"OL_W_ID": w_id, "OL_D_ID": d_id, "OL_O_ID": {
                                              "$lt": o_id, "$gte": o_id-20}}, {"OL_I_ID": 1})

        assert orderLines
        ol_ids = set()
        for ol in orderLines:
            ol_ids.add(ol["OL_I_ID"])
        # FOR
        result = self.stock.find({"S_W_ID": w_id, "S_I_ID": {"$in": list(
            ol_ids)}, "S_QUANTITY": {"$lt": threshold}}).count()

        return int(result)

# CLASS
