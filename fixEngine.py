#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 28 08:24:13 2020

@author: hugo.nistal.gonzalez
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
logging.basicConfig(filename=sys.stdout, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class FixEngine(FIXConnectionHandler):

    def __init__(self, senderCompID, targetCompID, username, password, reader, writer, messageListener, fixVersion="FIX.4.4", heartbeatInterval=10, readTimeOut=30, writeTimeOut=10):
        FIXConnectionHandler.__init__(self, senderCompID, targetCompID, reader, writer, messageListener, fixVersion)
        self._senderCompID = senderCompID
        self._targetCompID = targetCompID
        self._username = username
        self._password = password
        self._heartbeatInterval = heartbeatInterval
        self._logout = False
        self._clientMessage = FixClientMessages(senderCompID, targetCompID, username, password, fixVersion, heartbeatInterval)
        asyncio.ensure_future(self.logon())
        asyncio.ensure_future(self.handleHearbeat())
    
    def getConnectionState(self):
        return self._connectionState

    def sessionMessageHandler(self, message: simplefix.FixMessage) -> bool:
        """ Handle Session Message."""
        assert isinstance(message, simplefix.FixMessage)

        msgType = message.get(35)
        print("Processing business Message")
        print(msgType)
        if msgType == b'A': # Handle logon
            if self._connectionState == SocketConnectionState.LOGGED_IN:
                logger.warning(f"{self._senderCompID} already looged in -> Ignoring Login Request.")
            else:
                self._connectionState = SocketConnectionState.LOGGED_IN
                self._heartbeatInterval = float(message.get(108))
                return True
        elif self._connectionState == SocketConnectionState.LOGGED_IN:
            if msgType == b'1': # Send test heartbeat when requested
                msg = self._clientMessage.sendHeartbeat()
                msg.append_pair(112, message.get(112))
                self.sendMessage(msg)
                return True
            elif msgType == b'5': # Handle Logout
                self._connectionState = SocketConnectionState.LOGGED_OUT
                self.handleClose()
                return True
            elif message.get(141) == b'Y': # If ResetSeqNum = Y Then Reset sequence
                self._sequenceNum = 1
                logger.info("Resetting Sequence Number to 1")
                return True
            else:
                return False
        else:
            logger.warning(f"Cannot process message. {self._senderCompID} is not logged in.")
            return False

    async def logon(self):
        print("Logon")
        msg = self._clientMessage.sendLogOn()
        await self.sendMessage(msg)

    async def handleHearbeat(self):
        while True:
            print("Start wait")
            await asyncio.sleep(self._heartbeatInterval)
            print("Finish wait")
            print(self._connectionState)
            if self._connectionState == SocketConnectionState.LOGGED_IN:
                msg = self._clientMessage.sendHeartbeat()
                await self.sendMessage(msg)

            
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