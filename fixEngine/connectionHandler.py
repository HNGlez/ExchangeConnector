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

import asyncio
import sys
import simplefix
import logging
import time
from enum import Enum
from sessionHandler import FIXSessionHandler
import queue
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(filename='logs/fix_logs.log', format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class SocketConnectionState(Enum):
    UNKOWN = 0
    LOGGED_IN = 1
    CONNECTED = 2
    LOGGED_OUT = 3
    DISCONNECTED = 4

class FIXConnectionHandler(object):
    def __init__(self, senderCompID, targetCompID, reader, writer, messageListener, fixVersion="FIX.4.4", maxMissedHeartbeats=3, logonInterval=1, maxLogonAttempts=5):
        self._connectionState = SocketConnectionState.CONNECTED
        self._senderCompID = senderCompID
        self._reader = reader
        self._writer = writer
        self._sequenceNum = 0
        self.fixParser = simplefix.FixParser()
        self._fixProtocol = fixVersion
        self._session = FIXSessionHandler(targetCompID, senderCompID)
        self.clientMessage = None
        self._listener = messageListener
        self._lastRcvMsg = None
        self._missedHeartbeats = 0
        self._maxMissedHeartbeats = maxMissedHeartbeats
        self._logonCount = 0
        self._logonInterval = logonInterval
        self._maxLogonAttempts = maxLogonAttempts
        self._bufferSize = 128

    async def disconnect(self):
        """ Disconnect Session."""
        if self._connectionState != SocketConnectionState.LOGGED_OUT:
            await self.logout()
        await self.handleClose()

    async def handleClose(self):
        """ Handle Close Writer Socket Connection."""
        if self._connectionState == SocketConnectionState.DISCONNECTED:
            logger.info(f"{self._senderCompID} session -> DISCONNECTED")
            self._writer.close()
            self._connectionState = SocketConnectionState.DISCONNECTED
    
    async def sendMessage(self, message: simplefix.FixMessage):
        """ Send FIX Message to Server."""
        if self._connectionState != SocketConnectionState.CONNECTED and self._connectionState != SocketConnectionState.LOGGED_IN:
            return
        self._session.sequenceNumHandler(message)
        self._writer.write(message.encode())
        await self._writer.drain()
        self._lastMsgSent = time.time()

        logger.info(f"Client Sending Message: {FIXConnectionHandler.printFix(message.encode())}")
        print(f"Client Sending Message:   {FIXConnectionHandler.printFix(message.encode())}")
    
    async def readMessage(self):
        try:
            message = None
            print("Reading")
            while message is None:
                buffer = await self._reader.read(self._bufferSize)
                if not buffer:
                    break

                self.fixParser.append_buffer(buffer)
                message = self.fixParser.get_message()

            if message is None:
                return
            logger.info(f"{self._senderCompID} Received Message: {message}")
            print("Receiving", message)
            assert isinstance(message, simplefix.FixMessage)
            await self.processMessage(message)
        except ConnectionError as e:
            logger.error("Connection Closed.", exc_info=True)
            print(e)
        except asyncio.CancelledError as e:
            logger.error(f"{self._senderCompID} Read Message Timed Out")
            print(e)
    
    async def processMessage(self, message: simplefix.FixMessage):
        print("Processing Message")
        
        self._lastRcvMsg = time.time()
        self._missedHeartbeats = 0
        beginString = message.get(8).decode()

        if beginString != self._fixProtocol:
            logger.warning(f"FIX Protocol is incorrect. Expected: {self._fixProtocol}; Received: {beginString}")
            await self.disconnect()
            return
        
        if not await self._sessionMessageHandler(message):
            await self.messageNotification(message)
        recvSeqNo = message.get(34).decode()

        seqNoState, expectedSeqNo = self._session.validateRecvSeqNo(recvSeqNo)
        if seqNoState == False:
            # Unexpected sequence number. Send resend request
            logger.info(f"Sending Resend Request of messages: {expectedSeqNo} to {recvSeqNo}")
            msg = self.clientMessage.sendResendRequest(expectedSeqNo, recvSeqNo)
            await self.sendMessage(msg)
        else:
            self._session.updateRecvSeqNo(recvSeqNo)

    async def messageNotification(self, message):
        await self._listener(message)

    async def logon(self):
        if self._logonCount >= self._maxLogonAttempts:
            logger.warning("Max Logon attemps reached. Disconnecting")
            self.disconnect()
        logger.info(f"{self._senderCompID} session -> Sending LOGON")
        print("Logon")
        msg = self.clientMessage.sendLogOn()
        await self.sendMessage(msg)
        await asyncio.sleep(self._logonInterval)

    async def logout(self):
        logger.info(f"{self._senderCompID} session -> Sending LOGOUT")
        print("logout")
        self._connectionState = SocketConnectionState.LOGGED_OUT
        await self.sendMessage(self.clientMessage.sendLogOut())

    async def expectedHeartbeat(self, heartbeatInterval):
        if self._lastRcvMsg is None:
            return
        if time.time() - self._lastRcvMsg > heartbeatInterval:
            logger.warning(f"Heartbeat expected not received. Missed Heartbeats: {self._missedHeartbeats}")
            self._missedHeartbeats += 1
        if self._missedHeartbeats >= self._maxMissedHeartbeats:
            logger.warning(f"Max Missed Heartbeats reached. Loging out")
            self.logout()

        
    # Template functions to be overwritten in child class
    def _sessionMessageHandler(self, message):
        return -1
    
    @staticmethod
    def printFix(msg):
        return msg.replace(b"\x01", b"|")
