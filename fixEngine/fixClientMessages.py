#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ExchangeConnector fixEngine

Copyright (c) 2020 Hugo Nistal Gonzalez

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import simplefix
import time

class FixClientMessages():
    def __init__(self, senderCompID, targetComptID, password, fixVersion, heartbeatInterval):
        self._senderCompID = senderCompID
        self._targetCompID = targetComptID
        self._password = password
        self._fixVersion = fixVersion
        self._heartbeatInterval = heartbeatInterval
        self._requestID = None

    def createMessage(self, messageType: bytes) -> simplefix.FixMessage():
        """ Creates Basic Structure of FIX Message. """
        assert isinstance(messageType, bytes)
        assert len(messageType) == 1

        msg = simplefix.FixMessage()
        msg.append_pair(simplefix.TAG_BEGINSTRING, self._fixVersion)
        msg.append_pair(simplefix.TAG_MSGTYPE, messageType)
        msg.append_pair(simplefix.TAG_SENDER_COMPID, self._senderCompID)
        msg.append_pair(simplefix.TAG_TARGET_COMPID, self._targetCompID)
        msg.append_utc_timestamp(simplefix.TAG_SENDING_TIME, header=True)
        return msg

    # Business Mesages
    def sendLogOn(self, resetSeqNo=simplefix.RESETSEQNUMFLAG_YES):
        msg = self.createMessage(simplefix.MSGTYPE_LOGON)
        msg.append_pair(simplefix.TAG_ENCRYPTMETHOD, simplefix.ENCRYPTMETHOD_NONE)
        msg.append_pair(simplefix.TAG_HEARTBTINT, self._heartbeatInterval)
        msg.append_pair(simplefix.TAG_RESETSEQNUMFLAG, resetSeqNo)
        msg.append_pair(554, self._password) # Password
        return msg
    
    def sendLogOut(self):
        msg = self.createMessage(simplefix.MSGTYPE_LOGOUT)
        return msg

    def sendResendRequest(self, beginSeqNo, endSeqNo):
        msg = self.createMessage(simplefix.MSGTYPE_RESEND_REQUEST)
        msg.append_pair(simplefix.TAG_BEGINSEQNO, beginSeqNo)
        msg.append_pair(simplefix.TAG_ENDSEQNO, endSeqNo)
        return msg

    def sendHeartbeat(self):
        return self.createMessage("0")

    def sendChangePasswordRequest(self, newPassword):
        msg = self.createMessage(simplefix.MSGTYPE_USER_REQUEST)
        msg.append_pair(924, "3")
        msg.append_pair(553, self._senderCompID)
        msg.append_pair(554, self._password)
        msg.append_pair(925, newPassword)
        return msg

    # TradeCaptureReport Messages (Drop Copy Gateway messages)
    def sendTradeCaptureReportRequest(self, updatesOnly=False):
        msg = self.createMessage(simplefix.MSGTYPE_TRADE_CAPTURE_REPORT_REQUEST)
        self._requestID = str(time.time())
        msg.append_pair(568, self._requestID)
        msg.append_pair(569, "0")
        if not updatesOnly:
            msg.append_pair(263, "1")
        else:
            msg.append_pair(263, "9")
        return msg

    def sendTradeCaptureReportAck(self, tradeReportID):
        msg = self.createMessage(simplefix.MSGTYPE_TRADE_CAPTURE_REPORT_ACK)
        msg.append_pair(571, tradeReportID)
        msg.append_pair(simplefix.TAG_SYMBOL, "NA")
        return msg

    # Market Data messages (Market Data Gateway messages)
    def marketDataRequest(self, symbols, requestType, bookDepth, aggregateBook, unsubscribeFrom=None, correlation=None):
        msg = self.createMessage(simplefix.MSGTYPE_MARKET_DATA_REQUEST)
        assert isinstance(symbols, list)

        mdCorrelation = None
        if requestType == "2":
            assert unsubscribeFrom is not None
            assert correlation is not None
            msg.append_pair(262, correlation) # MDReqID

        else:
            mdCorrelation = {f"{'_'.join(symbols)}_{str(requestType)}": str(int(round(time.time() * 1000)))}
            msg.append_pair(262, mdCorrelation) # MDReqID
        msg.append_pair(263, requestType) # SubscriptionRequestType
        msg.append_pair(264, bookDepth) # MarketDepth
        msg.setField(265, 1) # MDUpdateType
        if requestType != "T" and unsubscribeFrom != "T":
            msg.append_pair(266, aggregateBook) # AggregatedBook

        if requestType == "1" or unsubscribeFrom == "1":
            msg.append_pair(267, 2) # NoMDEntryTypes (Repeating Group)
            msg.append_pair(269, "0") # MDEntryType
            msg.append_pair(269, "1")
        elif requestType == "T" or unsubscribeFrom == "T":
            msg.append_pair(267, 1)
            msg.append_pair(269, "2")

        msg.append_pair(146, len(symbols)) # NoRelatedSym (Repeating Group)
        for sym in symbols:
            msg.append_pair(simplefix.TAG_SYMBOL, sym) # Symbol

        if mdCorrelation is not None:
            return msg, mdCorrelation
        else:
            return msg
    
    # Order Management messages (Order Management Gateway messages)
    def newOrderSingle(self, clOrdID, partyID, partyRole, currency, side, symbol, quantity, price, orderType, product, tif, execInst=None, stopPrice=None, expiryDate=None, minQty=None, accountType=None, custOrderCapacity=None, precision=6):

        msg = self.createMessage(simplefix.MSGTYPE_NEW_ORDER_SINGLE)
        msg.append_pair(simplefix.TAG_CLORDID, clOrdID)

        msg.append_pair(453, 1) # NoPartyIDs (Repeating Group)
        msg.append_pair(448, partyID) # PartyID
        msg.append_pair(452, partyRole) # PartyRole

        if accountType is not None:
            msg.append_pair(581, accountType) # AccountType
        if custOrderCapacity is not None:
            msg.append_pair(582, custOrderCapacity) # CustOrderCapacity
        msg.append_pair(simplefix.TAG_HANDLINST, simplefix.HANDLINST_AUTO_PRIVATE)
        if execInst is not None:
            msg.append_pair(simplefix.TAG_EXECINST, execInst)
        msg.append_pair(simplefix.TAG_CURRENCY, currency)
        msg.append_pair(simplefix.TAG_SIDE, side)
        msg.append_pair(simplefix.TAG_SYMBOL, symbol)
        msg.append_pair(460, product) # Producct
        msg.append_utc_timestamp(simplefix.TAG_TRANSACTTIME, precision=precision)
        msg.append_pair(simplefix.TAG_ORDERQTY, quantity)
        msg.append_pair(simplefix.TAG_ORDTYPE, orderType)
        msg.append_pair(simplefix.TAG_PRICE, price)
        if orderType == simplefix.ORDTYPE_STOP_LIMIT:
            assert stopPrice is not None
            msg.append_pair(simplefix.TAG_STOPPX, stopPrice)
        if tif == simplefix.TIMEINFORCE_GOOD_TILL_DATE:
            assert expiryDate is not None
            msg.append_pair(432, expiryDate) # ExpireDate
        msg.append_pair(simplefix.TAG_TIMEINFORCE, tif)
        if minQty != None:
            assert tif == simplefix.TIMEINFORCE_IMMEDIATE_OR_CANCEL
            msg.append_pair(simplefix.TAG_MINQTY, minQty)

        return msg

    def orderCancelReplaceRequest(self, clOrdID, orderID, origClOrdID, side, symbol, price, orderType, quantity=None,  currency=None, product=None, tif=None, execInst=None, stopPrice=None, expiryDate=None, minQty=None, overfillProtection=None, accountType=None, custOrderCapacity=None, precision=6):

        msg = self.createMessage(simplefix.MSGTYPE_ORDER_CANCEL_REPLACE_REQUEST)
        msg.append_pair(simplefix.TAG_ORDERID, orderID)
        msg.append_pair(simplefix.TAG_ORIGCLORDID, origClOrdID)
        msg.append_pair(simplefix.TAG_CLORDID, clOrdID)

        if accountType is not None:
            msg.append_pair(581, accountType) # AccountType
        if custOrderCapacity is not None:
            msg.append_pair(582, custOrderCapacity) # CustOrderCapacity
        msg.append_pair(simplefix.TAG_HANDLINST, simplefix.HANDLINST_AUTO_PRIVATE)
        if execInst is not None:
            msg.append_pair(simplefix.TAG_EXECINST, execInst)
        if currency is not None:
            msg.append_pair(simplefix.TAG_CURRENCY, currency)
        msg.append_pair(simplefix.TAG_SIDE, side)
        msg.append_pair(simplefix.TAG_SYMBOL, symbol)
        if product is not None:
            msg.append_pair(460, product) # Producct
        msg.append_utc_timestamp(simplefix.TAG_TRANSACTTIME, precision=precision)
        if quantity is not None:
            msg.append_pair(simplefix.TAG_ORDERQTY, quantity)
        msg.append_pair(simplefix.TAG_ORDTYPE, orderType)
        msg.append_pair(simplefix.TAG_PRICE, price)
        if orderType == simplefix.ORDTYPE_STOP_LIMIT:
            assert stopPrice is not None
            msg.append_pair(simplefix.TAG_STOPPX, stopPrice)
        if tif == simplefix.TIMEINFORCE_GOOD_TILL_DATE and expiryDate is not None:
            msg.append_pair(432, expiryDate) # ExpireDate
        if tif is not None:
            msg.append_pair(simplefix.TAG_TIMEINFORCE, tif)
        if minQty is not None:
            assert tif == simplefix.TIMEINFORCE_IMMEDIATE_OR_CANCEL
            msg.append_pair(simplefix.TAG_MINQTY, minQty)
        if overfillProtection is not None:
            msg.append_pair(5000, overfillProtection) # Overfill Protection

        return msg

    def orderCancelRequest(self, cancelAll=False, clOrdID=None, orderID=None, origClOrdID=None, side=None, symbol=None, orderType=None):
        msg = self.createMessage(simplefix.MSGTYPE_ORDER_CANCEL_REQUEST)
        assert isinstance(cancelAll, bool)
        if cancelAll == True:
            msg.append_pair(simplefix.TAG_ORDERID, "OPEN_ORDER")
            msg.append_pair(simplefix.TAG_ORIGCLORDID, "OPEN_ORDER")
            msg.append_pair(simplefix.TAG_CLORDID, "OPEN_ORDER")
            msg.append_pair(simplefix.TAG_SYMBOL, "NA")
            msg.append_pair(simplefix.TAG_SIDE, "1")
            msg.append_pair(7559, "Y")
        else:
            assert clOrdID is not None
            assert orderID is not None
            assert origClOrdID is not None
            assert side is not None
            assert symbol is not None
            msg.append_pair(simplefix.TAG_ORDERID, orderID)
            msg.append_pair(simplefix.TAG_ORIGCLORDID, origClOrdID)
            msg.append_pair(simplefix.TAG_CLORDID, clOrdID)
            msg.append_pair(simplefix.TAG_SYMBOL, symbol)
            msg.append_pair(simplefix.TAG_SIDE, side)
        msg.append_utc_timestamp(simplefix.TAG_TRANSACTTIME)
        if orderType is not None:
            msg.append_pair(simplefix.TAG_ORDTYPE, orderType)
        
        return msg

    def orderMassStatusRequest(self):
        msg = self.createMessage(simplefix.MSGTYPE_ORDER_MASS_STATUS_REQUEST)
        requestID = str(int(round(time.time() * 1000)))
        msg.append_pair(584, requestID)
        msg.append_pair(585, 8)
        msg.append_utc_timestamp(simplefix.TAG_TRANSACTTIME)

        return msg, requestID
    



    





    
