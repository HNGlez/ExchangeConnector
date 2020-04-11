#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 11 12:11:45 2020

@author: hugo.nistal.gonzalez
"""
import logging
import simplefix
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(filename='logs/fix_logs.log', format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class FIXSessionHandler:
    def __init__(self, targetCompID, senderCompID):
        self._targetCompID = targetCompID
        self._senderCompID = senderCompID

        self._outboundSeqNo = 0
        self._nextExpectedSeqNo = 0

    def validateCompIDs(self, targetCompID, senderCompID):
        return self._targetCompID == targetCompID and self._senderCompID == senderCompID
    
    def validateRecvSeqNo(self, msgSeqNo):
        if self._nextExpectedSeqNo < int(msgSeqNo):
            logger.warning(f"Received Sequence Number not expected. Received: {msgSeqNo}; Expected {self._nextExpectedSeqNo}")
            return False, self._nextExpectedSeqNo
        else:
            return True, msgSeqNo
    
    def resetSeqNo(self):
        self._outboundSeqNo = 0
        self._nextExpectedSeqNo = 1
    
    def updateRecvSeqNo(self, msgSeqNo):
        self._nextExpectedSeqNo = int(msgSeqNo) + 1

    def sequenceNumHandler(self, message: simplefix.FixMessage):
        """ Append Correct Sequence Number to FIX Message."""
        assert isinstance(message, simplefix.FixMessage)
        self._outboundSeqNo += 1
        message.append_pair(34, self._outboundSeqNo, header=True)        