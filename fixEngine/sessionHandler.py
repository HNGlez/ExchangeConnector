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
import logging
import simplefix
from os import path
import json

class FIXSessionHandler:
    def __init__(self, targetCompID, senderCompID, resetSeqNum, enginelogger, logPath):
        self._targetCompID = targetCompID
        self._senderCompID = senderCompID
        self._engineLogger = enginelogger
        if resetSeqNum == "Y":
            self._outboundSeqNo = 0
            self._nextExpectedSeqNo = 1
        elif resetSeqNum == "N" and path.exists(f"{logPath}/{self._senderCompID}-seqNums.json"):
            with open(f"{logPath}/{self._senderCompID}-seqNums.json", 'r') as f:
                seqNums = json.load(f)
                self._outboundSeqNo = seqNums["outboundSeqNo"]
                self._nextExpectedSeqNo = seqNums["nextExpectedSeqNo"]
        else:
            self._engineLogger.error("resetSeqNum Flag not set to Y but previous sequence numbers not found")
            raise Exception(f"File not found: {logPath}/{self._senderCompID}-seqNums.json")

            

    def getOutboundSeqNo(self):
        return self._outboundSeqNo
    
    def getNextExpectedSeqNo(self):
        return self._nextExpectedSeqNo

    def validateCompIDs(self, targetCompID, senderCompID):
        return self._targetCompID == targetCompID and self._senderCompID == senderCompID
    
    def validateRecvSeqNo(self, msgSeqNo):
        if self._nextExpectedSeqNo != int(msgSeqNo):
            self._engineLogger.warning(f"Received Sequence Number not expected. Received: {msgSeqNo}; Expected {self._nextExpectedSeqNo}")
            return False, self._nextExpectedSeqNo
        else:
            return True, msgSeqNo
    
    def resetSeqNo(self):
        self._outboundSeqNo = 0
        self._nextExpectedSeqNo = 1
        self._engineLogger.info("Resetting Both Sequence Numbers")

    def updateRecvSeqNo(self, msgSeqNo):
        self._nextExpectedSeqNo = int(msgSeqNo) + 1

    def sequenceNumHandler(self, message: simplefix.FixMessage):
        """ Append Correct Sequence Number to FIX Message."""
        assert isinstance(message, simplefix.FixMessage)
        self._outboundSeqNo += 1
        message.append_pair(34, self._outboundSeqNo, header=True)
