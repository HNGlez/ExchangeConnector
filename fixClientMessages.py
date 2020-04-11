#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 28 09:35:16 2020

@author: hugonistalgonzalez
"""

import simplefix
import time

class FixClientMessages():
    def __init__(self, senderCompID, targetComptID, username, password, fixVersion="FIX.4.4", heartbeatInterval=60):
        self._senderCompID = senderCompID
        self._targetCompID = targetComptID
        self._username = username
        self._password = password
        self._fixVersion = fixVersion
        self._heartbeatInterval = heartbeatInterval
        self._requestID = None

    def createMessage(self, messageType: str) -> simplefix.FixMessage():
        """ Creates Basic Structure of FIX Message. """
        assert isinstance(messageType, str)
        assert len(messageType) == 1

        msg = simplefix.FixMessage()
        msg.append_pair(8, self._fixVersion)
        msg.append_pair(35, messageType)
        msg.append_pair(49, self._senderCompID)
        msg.append_pair(56, self._targetCompID)
        msg.append_utc_timestamp(52, header=True)
        return msg


    def sendLogOn(self):
        msg = self.createMessage("A")
        msg.append_pair(98, "0")
        msg.append_pair(108, self._heartbeatInterval)
        msg.append_pair(141, "Y")
        msg.append_pair(554, self._password)
        return msg
    
    def sendLogOut(self):
        msg = self.createMessage("5")
        return msg

    def sendResendRequest(self, beginSeqNo, endSeqNo):
        msg = self.createMessage("2")
        msg.append_pair(7, beginSeqNo)
        msg.append_pair(16, endSeqNo)
        return msg

    def sendHeartbeat(self):
        return self.createMessage("0")

    def sendChangePasswordRequest(self, newPassword):
        msg = self.createMessage("BE")
        msg.append_pair(924, "3")
        msg.append_pair(553, self._senderCompID)
        msg.append_pair(554, self._password)
        msg.append_pair(925, newPassword)
        return msg

    def sendTradeCaptureReportRequest(self, updatesOnly=False):
        msg = self.createMessage("AD")
        self._requestID = str(time.time.now())
        msg.append_pair(568, self._requestID)
        msg.append_pair(569, "0")
        if not updatesOnly:
            msg.append_pair(263, "1")
        else:
            msg.append_pair(263, "9")
        return msg

    def sendTradeCaptureReportAck(self, tradeReportID):
        msg = self.createMessage("AR")
        msg.append_pair(571, tradeReportID)
        msg.append_pair(55, "NA")
        return msg
    
    



    





    
