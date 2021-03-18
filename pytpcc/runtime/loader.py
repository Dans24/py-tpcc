# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http:##www.cs.brown.edu/~pavlo/
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

import os
import sys

import logging
from datetime import datetime
from random import shuffle
from pprint import pprint,pformat

import constants
from util import *

class Loader:
    
    def __init__(self, handle, scaleParameters, w_ids, needLoadItems):
        self.handle = handle
        self.scaleParameters = scaleParameters
        self.w_ids = w_ids
        self.needLoadItems = needLoadItems
        self.batch_size = 2500
        
    ## ==============================================
    ## execute
    ## ==============================================
    def execute(self):
        
        ## Item Table
        if self.needLoadItems:
            logging.debug("Loading ITEM table")
            self.loadItems()
            self.handle.loadFinishItem()
            
        ## Then create the warehouse-specific tuples
        for w_id in self.w_ids:
            self.loadWarehouse(w_id)
        for w_id in self.w_ids:
            self.loadDistricts(w_id)
        for w_id in self.w_ids:
            self.loadWarehouseDeliveries(w_id)
        for w_id in self.w_ids:
            self.handle.loadFinishWarehouse(w_id)
        ## FOR
        
        return (None)

    ## ==============================================
    ## loadItems
    ## ==============================================
    def loadItems(self):
        ## Select 10% of the rows to be marked "original"
        originalRows = rand.selectUniqueIds(self.scaleParameters.items / 10, 1, self.scaleParameters.items)
        
        ## Load all of the items
        tuples = [ ]
        total_tuples = 0
        for i in range(1, self.scaleParameters.items+1):
            original = (i in originalRows)
            tuples.append(self.generateItem(i, original))
            total_tuples += 1
            if len(tuples) == self.batch_size:
                logging.debug("LOAD - %s: %5d / %d" % (constants.TABLENAME_ITEM, total_tuples, self.scaleParameters.items))
                self.handle.loadTuples(constants.TABLENAME_ITEM, tuples)
                tuples = [ ]
        ## FOR
        if len(tuples) > 0:
            logging.debug("LOAD - %s: %5d / %d" % (constants.TABLENAME_ITEM, total_tuples, self.scaleParameters.items))
            self.handle.loadTuples(constants.TABLENAME_ITEM, tuples)
    ## DEF

    ## ==============================================
    ## loadWarehouse
    ## ==============================================
    def loadWarehouse(self, w_id):
        logging.debug("LOAD - %s: %d / %d" % (constants.TABLENAME_WAREHOUSE, w_id, len(self.w_ids)))
        
        ## WAREHOUSE
        w_tuples = [ self.generateWarehouse(w_id) ]
        self.handle.loadTuples(constants.TABLENAME_WAREHOUSE, w_tuples)

        ## Select 10% of the stock to be marked "original"
        s_tuples = [ ]
        selectedRows = rand.selectUniqueIds(self.scaleParameters.items / 10, 1, self.scaleParameters.items)
        total_tuples = 0
        for i_id in range(1, self.scaleParameters.items+1):
            original = (i_id in selectedRows)
            s_tuples.append(self.generateStock(w_id, i_id, original))
            if len(s_tuples) >= self.batch_size:
                logging.debug("LOAD - %s [W_ID=%d]: %5d / %d" % (constants.TABLENAME_STOCK, w_id, total_tuples, self.scaleParameters.items))
                self.handle.loadTuples(constants.TABLENAME_STOCK, s_tuples)
                s_tuples = [ ]
            total_tuples += 1
        ## FOR
        if len(s_tuples) > 0:
            logging.debug("LOAD - %s [W_ID=%d]: %5d / %d" % (constants.TABLENAME_STOCK, w_id, total_tuples, self.scaleParameters.items))
            self.handle.loadTuples(constants.TABLENAME_STOCK, s_tuples)
    ## DEF

    ## ==============================================
    ## loadDistricts
    ## ==============================================
    def loadDistricts(self, w_id):
        logging.debug("LOAD - %s: %d / %d" % (constants.TABLENAME_WAREHOUSE, w_id, len(self.w_ids)))
  
        ## DISTRICT
        d_tuples = [ ]
        for d_id in range(1, self.scaleParameters.districtsPerWarehouse+1):
            d_tuples = [ self.generateDistrict(w_id, d_id) ]
            
            c_tuples = [ ]
            h_tuples = [ ]
            dl_tuples = [ ]
            dlo_tuples = [ ]

            ## Select 10% of the customers to have bad credit
            selectedRows = rand.selectUniqueIds(self.scaleParameters.customersPerDistrict / 10, 1, self.scaleParameters.customersPerDistrict)
            
            ## TPC-C 4.3.3.1. says that o_c_id should be a permutation of [1, 3000]. But since it
            ## is a c_id field, it seems to make sense to have it be a permutation of the
            ## customers. For the "real" thing this will be equivalent
            cIdPermutation = [ ]

            for c_id in range(1, self.scaleParameters.customersPerDistrict+1):
                badCredit = (c_id in selectedRows)
                c_tuples.append(self.generateCustomer(w_id, d_id, c_id, badCredit, True))
                h_tuples.append(self.generateHistory(w_id, d_id, c_id))
                cIdPermutation.append(c_id)
            ## FOR
            assert cIdPermutation[0] == 1
            assert cIdPermutation[self.scaleParameters.customersPerDistrict - 1] == self.scaleParameters.customersPerDistrict
            shuffle(cIdPermutation)
            
            o_tuples = [ ]
            ol_tuples = [ ]
            
            for o_id in range(1, self.scaleParameters.customersPerDistrict+1):
                o_ol_cnt = rand.number(constants.MIN_OL_CNT, constants.MAX_OL_CNT)
                o_entry_d = datetime.now()
                o_tuples.append(self.generateOrder(o_id, w_id, d_id, cIdPermutation[o_id - 1], o_entry_d))

                ## Generate each OrderLine for the order
                for ol_number in range(0, o_ol_cnt):
                    ol_tuples.append(self.generateOrderLine(o_id, w_id, d_id, ol_number, self.scaleParameters.items))
                ## FOR
            ## FOR
            
            self.handle.loadTuples(constants.TABLENAME_DISTRICT, d_tuples)
            self.handle.loadTuples(constants.TABLENAME_CUSTOMER, c_tuples)
            self.handle.loadTuples(constants.TABLENAME_ORDERS, o_tuples)
            self.handle.loadTuples(constants.TABLENAME_ORDER_LINE, ol_tuples)
            self.handle.loadTuples(constants.TABLENAME_HISTORY, h_tuples)
        ## FOR
        
    ## DEF

    def loadWarehouseDeliveries(self, w_id):
        logging.debug("LOAD - %s: %d / %d" % (constants.TABLENAME_WAREHOUSE, w_id, len(self.w_ids)))
    
        # The last newOrdersPerDistrict are new orders
        numDeliveries = (self.scaleParameters.customersPerDistrict - self.scaleParameters.newOrdersPerDistrict)

        dl_tuples = []
        dlo_tuples = []
        for o_id in range(1, numDeliveries + 1):
            dl_delivery_d = datetime.now()                
            dl_tuples.append(self.generateDelivery(dl_delivery_d, w_id))
            for d_id in range(1, self.scaleParameters.districtsPerWarehouse+1):
                dlo_tuples.append(self.generateDeliveryOrder(dl_delivery_d, w_id, o_id, d_id))
            ## FOR
        ## FOR

        self.handle.loadTuples(constants.TABLENAME_DELIVERY, dl_tuples)
        self.handle.loadTuples(constants.TABLENAME_DELIVERY_ORDERS, dlo_tuples)
        for d_id in range(1, self.scaleParameters.districtsPerWarehouse+1):
            self.handle.loadFinishDistrict(w_id, d_id)
    ## DEF

    ## ==============================================
    ## generateItem
    ## ==============================================
    def generateItem(self, id, original):
        i_id = id
        i_im_id = rand.number(constants.MIN_IM, constants.MAX_IM)
        i_name = rand.astring(constants.MIN_I_NAME, constants.MAX_I_NAME)
        i_price = rand.fixedPoint(constants.MONEY_DECIMALS, constants.MIN_PRICE, constants.MAX_PRICE)
        i_data = rand.astring(constants.MIN_I_DATA, constants.MAX_I_DATA)
        if original: i_data = self.fillOriginal(i_data)

        return [i_id, i_im_id, i_name, i_price, i_data]
    ## DEF

    ## ==============================================
    ## generateWarehouse
    ## ==============================================
    def generateWarehouse(self, w_id):
        w_tax = self.generateTax()
        w_address = self.generateAddress()
        return [w_id] + w_address + [w_tax]
    ## DEF

    ## ==============================================
    ## generateDistrict
    ## ==============================================
    def generateDistrict(self, d_w_id, d_id):
        d_tax = self.generateTax()
        d_address = self.generateAddress()
        return [d_id, d_w_id] + d_address + [d_tax]
    ## DEF

    ## ==============================================
    ## generateCustomer
    ## ==============================================
    def generateCustomer(self, c_w_id, c_d_id, c_id, badCredit, doesReplicateName):
        c_first = rand.astring(constants.MIN_FIRST, constants.MAX_FIRST)
        c_middle = constants.MIDDLE

        assert 1 <= c_id and c_id <= constants.CUSTOMERS_PER_DISTRICT
        if c_id <= 1000:
            c_last = rand.makeLastName(c_id - 1)
        else:
            c_last = rand.makeRandomLastName(constants.CUSTOMERS_PER_DISTRICT)

        c_phone = rand.nstring(constants.PHONE, constants.PHONE)
        c_since = datetime.now()
        c_credit = constants.BAD_CREDIT if badCredit else constants.GOOD_CREDIT
        c_credit_lim = constants.INITIAL_CREDIT_LIM
        c_discount = rand.fixedPoint(constants.DISCOUNT_DECIMALS, constants.MIN_DISCOUNT, constants.MAX_DISCOUNT)
        c_data = rand.astring(constants.MIN_C_DATA, constants.MAX_C_DATA)

        c_street1 = rand.astring(constants.MIN_STREET, constants.MAX_STREET)
        c_street2 = rand.astring(constants.MIN_STREET, constants.MAX_STREET)
        c_city = rand.astring(constants.MIN_CITY, constants.MAX_CITY)
        c_state = rand.astring(constants.STATE, constants.STATE)
        c_zip = self.generateZip()

        return [ c_id, c_d_id, c_w_id, c_first, c_middle, c_last, \
                c_street1, c_street2, c_city, c_state, c_zip, \
                c_phone, c_since, c_credit, c_credit_lim, c_discount, c_data ]
    ## DEF

    ## ==============================================
    ## generateOrder
    ## ==============================================
    def generateOrder(self, o_id, o_w_id, o_d_id, o_c_id, o_entry_d):
        return [ o_id, o_d_id, o_w_id, o_c_id, o_entry_d ]
    ## DEF

    ## ==============================================
    ## generateOrderLine
    ## ==============================================
    def generateOrderLine(self, ol_o_id, ol_w_id, ol_d_id, ol_number, max_items):
        ol_i_id = rand.number(1, max_items)
        ol_supply_w_id = ol_w_id
        ol_quantity = constants.INITIAL_QUANTITY

        ## 1% of items are from a remote warehouse
        remote = (rand.number(1, 100) == 1)
        if self.scaleParameters.warehouses > 1 and remote:
            ol_supply_w_id = rand.numberExcluding(self.scaleParameters.starting_warehouse,
                                                  self.scaleParameters.ending_warehouse,
                                                  ol_w_id)
        
        if ol_o_id < (constants.INITIAL_ORDERS_PER_DISTRICT - constants.INITIAL_NEW_ORDERS_PER_DISTRICT):
            ol_amount = 0.00
        else:
            ol_amount = rand.fixedPoint(constants.MONEY_DECIMALS, constants.MIN_AMOUNT, constants.MAX_PRICE * constants.MAX_OL_QUANTITY)

        ol_dist_info = rand.astring(constants.DIST, constants.DIST)

        return [ ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info ]
    ## DEF

    ## ==============================================
    ## generateDelivery
    ## ==============================================
    def generateDelivery(self, dl_delivery_d, dl_w_id):
        o_carrier_id = rand.number(constants.MIN_CARRIER_ID, constants.MAX_CARRIER_ID)
        return [ dl_delivery_d, dl_w_id, o_carrier_id ]
    ## DEF

    ## ==============================================
    ## generateDeliveryOrder
    ## ==============================================
    def generateDeliveryOrder(self, dlo_delivery_d, dlo_w_id, dlo_o_id, dlo_d_id):
        return [ dlo_delivery_d, dlo_w_id, dlo_o_id, dlo_d_id ]
    ## DEF

    ## ==============================================
    ## generateStock
    ## ==============================================
    def generateStock(self, s_w_id, s_i_id, original):

        s_data = rand.astring(constants.MIN_I_DATA, constants.MAX_I_DATA)
        if original: self.fillOriginal(s_data)

        s_dists = [ ]
        for i in range(0, constants.DISTRICTS_PER_WAREHOUSE):
            s_dists.append(rand.astring(constants.DIST, constants.DIST))
        
        return [ s_i_id, s_w_id ] + \
               s_dists + \
               [ s_data ]
    ## DEF

    ## ==============================================
    ## generateHistory
    ## ==============================================
    def generateHistory(self, h_c_w_id, h_c_d_id, h_c_id):
        h_w_id = h_c_w_id
        h_d_id = h_c_d_id
        h_date = datetime.now()
        h_amount = constants.INITIAL_AMOUNT
        h_data = rand.astring(constants.MIN_DATA, constants.MAX_DATA)
        return [ h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data ]
    ## DEF

    ## ==============================================
    ## generateAddress
    ## ==============================================
    def generateAddress(self):
        """
            Returns a name and a street address 
            Used by both generateWarehouse and generateDistrict.
        """
        name = rand.astring(constants.MIN_NAME, constants.MAX_NAME)
        return [ name ] + self.generateStreetAddress()
    ## DEF

    ## ==============================================
    ## generateStreetAddress
    ## ==============================================
    def generateStreetAddress(self):
        """
            Returns a list for a street address
            Used for warehouses, districts and customers.
        """
        street1 = rand.astring(constants.MIN_STREET, constants.MAX_STREET)
        street2 = rand.astring(constants.MIN_STREET, constants.MAX_STREET)
        city = rand.astring(constants.MIN_CITY, constants.MAX_CITY)
        state = rand.astring(constants.STATE, constants.STATE)
        zip = self.generateZip()

        return [ street1, street2, city, state, zip ]
    ## DEF

    ## ==============================================
    ## generateTax
    ## ==============================================
    def generateTax(self):
        return rand.fixedPoint(constants.TAX_DECIMALS, constants.MIN_TAX, constants.MAX_TAX)
    ## DEF

    ## ==============================================
    ## generateZip
    ## ==============================================
    def generateZip(self):
        length = constants.ZIP_LENGTH - len(constants.ZIP_SUFFIX)
        return rand.nstring(length, length) + constants.ZIP_SUFFIX
    ## DEF

    ## ==============================================
    ## fillOriginal
    ## ==============================================
    def fillOriginal(self, data):
        """
            a string with ORIGINAL_STRING at a random position
        """
        originalLength = len(constants.ORIGINAL_STRING)
        position = rand.number(0, len(data) - originalLength)
        out = data[:position] + constants.ORIGINAL_STRING + data[position + originalLength:]
        assert len(out) == len(data)
        return out
    ## DEF
## CLASS
