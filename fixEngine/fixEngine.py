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
import simplefix
import threading
import logging
import time
import sys
from fixClientMessages import FixClientMessages
from connectionHandler import FIXConnectionHandler, SocketConnectionState

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class FixEngine(FIXConnectionHandler):

    def __init__(self, senderCompID, targetCompID, username, password, reader, writer, messageListener, fixVersion="FIX.4.4", heartbeatInterval=10, readTimeOut=30, writeTimeOut=10):
        FIXConnectionHandler.__init__(self, senderCompID, targetCompID, reader, writer, messageListener, fixVersion)
        self._senderCompID = senderCompID
        self._targetCompID = targetCompID
        self._username = username
        self._password = password
        self._heartbeatInterval = heartbeatInterval
        self._logout = False
        self.clientMessage = FixClientMessages(senderCompID, targetCompID, username, password, fixVersion, heartbeatInterval)
        asyncio.ensure_future(self._handleEngine())
    
    def getConnectionState(self):
        return self._connectionState

    async def _sessionMessageHandler(self, message: simplefix.FixMessage) -> bool:
        """ Handle Session Message."""
        assert isinstance(message, simplefix.FixMessage)

        msgType = message.get(simplefix.TAG_MSGTYPE)
        if msgType == simplefix.MSGTYPE_LOGON: # Handle logon
            if self._connectionState == SocketConnectionState.LOGGED_IN:
                logger.warning(f"{self._senderCompID} already looged in -> Ignoring Login Request.")
            else:
                self._connectionState = SocketConnectionState.LOGGED_IN
                self._heartbeatInterval = float(message.get(simplefix.TAG_HEARTBTINT))
                return True
        elif self._connectionState == SocketConnectionState.LOGGED_IN:
            if msgType == simplefix.MSGTYPE_TEST_REQUEST: # Send test heartbeat when requested
                msg = self.clientMessage.sendHeartbeat()
                msg.append_pair(simplefix.TAG_TESTREQID, message.get(simplefix.TAG_TESTREQID))
                await self.sendMessage(msg)
                return True
            elif msgType == simplefix.MSGTYPE_LOGOUT: # Handle Logout
                self._connectionState = SocketConnectionState.LOGGED_OUT
                self.handleClose()
                return True
            elif msgType == simplefix.MSGTYPE_HEARTBEAT:
                msg = self.clientMessage.sendHeartbeat()
                msg.append_pair(simplefix.TAG_TESTREQID, message.get(simplefix.TAG_TESTREQID))
                await self.sendMessage(msg)
                return True
            elif message.get(simplefix.TAG_RESETSEQNUMFLAG) == simplefix.RESETSEQNUMFLAG_YES: # If ResetSeqNum = Y Then Reset sequence
                self._session.resetSeqNo()
                logger.info("Resetting Sequence Number to 1")
                return True
            else:
                return False
        else:
            logger.warning(f"Cannot process message. {self._senderCompID} is not logged in.")
            return False

    async def _handleEngine(self):
        await self.logon()

        while self._connectionState != SocketConnectionState.DISCONNECTED:
            if self._connectionState != SocketConnectionState.LOGGED_OUT:
                await self.readMessage()
                await self.expectedHeartbeat(self._heartbeatInterval)
            else:
                await self.logon()

            
class FIXClient:
    def __init__(self, host, port, senderCompID, targetCompID, username, password, listener=None, fixVersion="FIX.4.4", heartbeatInterval=10, readTimeOut=30, writeTimeOut=10):
        self._host = host
        self._port = port
        self._senderCompID = senderCompID
        self._targetCompID = targetCompID
        self._username = username
        self._password = password
        self._fixVersion = fixVersion
        self._heartbeatInterval = heartbeatInterval
        self._readTimeOut = readTimeOut
        self._writeTimeOut = writeTimeOut
        self._reader = None
        self._writer = None
        self._client = None
        self._messageListener = listener

    async def startClient(self, host, port, loop):
        """ Creates Socket Connection and Runs Main Loop."""
        self._reader, self._writer = await asyncio.open_connection(self._host, self._port, loop=loop)
        self._connectionState = SocketConnectionState.CONNECTED
        logger.info(f"Socket Connection Open to {self._host}:{self._port}")
        print(f"Socket Connection Open to {self._host}:{self._port}")

        self._client = FixEngine(self._senderCompID, self._targetCompID, self._username, self._password, self._reader, self._writer, self._messageListener, heartbeatInterval=10, readTimeOut=30, writeTimeOut=10)

    def getClient(self):
        return self._client