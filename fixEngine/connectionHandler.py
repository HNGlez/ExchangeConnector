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

class SocketConnectionState(Enum):
    UNKOWN = 0
    LOGGED_IN = 1
    CONNECTED = 2
    LOGGED_OUT = 3
    DISCONNECTED = 4

class FIXConnectionHandler(object):
    def __init__(self, config, reader, writer, messageListener):
        self._config = config
        self._connectionState = SocketConnectionState.CONNECTED
        self._reader = reader
        self._writer = writer
        self._sequenceNum = 0
        self.fixParser = simplefix.FixParser()
        self._session = FIXSessionHandler(config["TargetCompID"], config["SenderCompID"])
        self.clientMessage = None
        self._listener = messageListener
        self._lastRcvMsg = None
        self._missedHeartbeats = 0
        self._lastLogonAttempt = 0
        self._logonCount = 0
        self._bufferSize = 128
        self._fixLogger = self.setupLogger(name=config["BeginString"],filename=f"{config['SenderCompID']}-fixMessages")
        self._engineLogger = self.setupLogger(name=config["SenderCompID"], filename=f"{config['SenderCompID']}-session", formatter="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


    def setupLogger(self, name, filename, level=logging.INFO, formatter="%(asctime)s - %(message)s"):
        handler = logging.FileHandler(filename=f"{self._config['FileLogPath']}/{filename}.log")
        handler.setFormatter(logging.Formatter(formatter))
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)
        return logger

    async def disconnect(self):
        """ Disconnect Session."""
        if self._connectionState != SocketConnectionState.LOGGED_OUT:
            await self.logout()
        await self.handleClose()

    async def handleClose(self):
        """ Handle Close Writer Socket Connection."""
        if self._connectionState == SocketConnectionState.DISCONNECTED:
            self._engineLogger.info(f"{self._config['SenderCompID']} session -> DISCONNECTED")
            self._writer.close()
            self._connectionState = SocketConnectionState.DISCONNECTED
    
    async def sendMessage(self, message: simplefix.FixMessage):
        """ Send FIX Message to Server."""
        if self._connectionState != SocketConnectionState.CONNECTED and self._connectionState != SocketConnectionState.LOGGED_IN:
            self._engineLogger.warning("Cannot Send Message. Socket is closed or Session is LOGGED OUT")
            return
        self._session.sequenceNumHandler(message)
        message = message.encode()
        self._writer.write(message)
        await self._writer.drain()
        self._lastMsgSent = time.time()
        self._fixLogger.info(f"{FIXConnectionHandler.printFix(message)}")
    
    async def readMessage(self):
        try:
            message = None
            while message is None:
                buffer = await self._reader.read(self._bufferSize)
                if not buffer:
                    break

                self.fixParser.append_buffer(buffer)
                message = self.fixParser.get_message()

            if message is None:
                return
            self._fixLogger.info(f"{message}")
            assert isinstance(message, simplefix.FixMessage)
            await self.processMessage(message)
        except ConnectionError as e:
            self._engineLogger.error("Connection Closed Unexpected.", exc_info=True)
            raise e
        except asyncio.CancelledError as e:
            self._engineLogger.error(f"{self._config['SenderCompID']} Read Message Timed Out")
            raise e
        except Exception as e:
            self._engineLogger.error("Error reading message", exc_info=True)
            raise e
    
    async def processMessage(self, message: simplefix.FixMessage):
        print("Processing Message")
        
        self._lastRcvMsg = time.time()
        self._missedHeartbeats = 0
        beginString = message.get(8).decode()

        if beginString != self._config['BeginString']:
            self._engineLogger.warning(f"FIX Protocol is incorrect. Expected: {self._config['BeginString']}; Received: {beginString}")
            await self.disconnect()
            return
        
        if not await self._sessionMessageHandler(message):
            await self.messageNotification(message)
        recvSeqNo = message.get(34).decode()

        seqNoState, expectedSeqNo = self._session.validateRecvSeqNo(recvSeqNo)
        if seqNoState == False:
            # Unexpected sequence number. Send resend request
            self._engineLogger.info(f"Sending Resend Request of messages: {expectedSeqNo} to {recvSeqNo}")
            msg = self.clientMessage.sendResendRequest(expectedSeqNo, recvSeqNo)
            await self.sendMessage(msg)
        else:
            self._session.updateRecvSeqNo(recvSeqNo)

    async def messageNotification(self, message):
        await self._listener(message)

    async def logon(self):
        if self._logonCount >= self._config.getint('MaxReconnectAttemps'):
            self._engineLogger.warning("Max Logon attemps reached. Disconnecting")
            self.disconnect()
        self._engineLogger.info(f"{self._config['SenderCompID']} session -> Sending LOGON")
        if time.time() - self._lastLogonAttempt > self._config.getint('ReconnectInterval'):
            msg = self.clientMessage.sendLogOn()
            await self.sendMessage(msg)
            self._lastLogonAttempt = time.time()

    async def logout(self):
        self._engineLogger.info(f"{self._config['SenderCompID']} session -> Sending LOGOUT")
        self._connectionState = SocketConnectionState.LOGGED_OUT
        await self.sendMessage(self.clientMessage.sendLogOut())

    async def expectedHeartbeat(self, heartbeatInterval):
        if self._lastRcvMsg is None:
            return
        if time.time() - self._lastRcvMsg > heartbeatInterval:
            self._engineLogger.warning(f"Heartbeat expected not received. Missed Heartbeats: {self._missedHeartbeats}")
            self._missedHeartbeats += 1
        if self._missedHeartbeats >= self._config.getint('MaxMissedHeartBeats'):
            self._engineLogger.warning(f"Max Missed Heartbeats reached. Loging out")
            self.logout()

        
    # Template functions to be overwritten in child class
    def _sessionMessageHandler(self, message):
        return -1
    
    @staticmethod
    def printFix(msg):
        return msg.replace(b"\x01", b"|").decode()
