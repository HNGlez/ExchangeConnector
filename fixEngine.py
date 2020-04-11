#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 28 08:24:13 2020

@author: hugo.nistal.gonzalez
"""

import asyncio
import simplefix
import queue
import threading
import logging
import time
from enum import Enum
from fixClientMessages import FixClientMessages

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(filename='logs/fix_logs.log', format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class SocketConnectionState(Enum):
    UNKOWN = 0
    LOGGED_IN = 1
    CONNECTED = 2
    LOGGED_OUT = 3
    DISCONNECTED = 4


class FixEngine:


    def __init__(self, host, port, senderCompID, targetCompID, username, password, fixVersion="FIX.4.4", heartbeatInterval=10, readTimeOut=30, writeTimeOut=10):
        self._host = host
        self._port = port
        self._senderCompID = senderCompID
        self._targetCompID = targetCompID
        self._username = username
        self._password = password
        self._fixVersion = fixVersion
        self._heartbeatInterval = heartbeatInterval
        self._sequenceNum = 0
        self._inboundSeqNum = 1
        self._connectionState = SocketConnectionState.DISCONNECTED
        self._logout = False
        self._last_hb = 0
        self.session_loop = None

        self.fixParser = simplefix.FixParser()
        self._reader = None
        self._writer = None
        self._writer_queue = queue.Queue()
        self._reader_queue = queue.Queue()
        self._clientMessage = FixClientMessages(senderCompID, targetCompID, username, password, fixVersion, heartbeatInterval)


    
    def timeToSendHb(self):
        if self._last_hb == 0 or time.time() - self._last_hb > self._heartbeatInterval:
            return True
        else:
            return False

    async def _readMessage(self):
        """ Reads Next Message from the Server."""

        try:
            message = None
            buffer = await self._reader.read(4096)
            if not buffer:
                raise ConnectionError

            self.fixParser.append_buffer(buffer)
            message = self.fixParser.get_message()
            if message is None:
                return
            logger.info(f"Client Received Message {message}")
            print(f"Client Received Message {message}")
            assert isinstance(message, simplefix.FixMessage)

            # Handle Inboud Sequence Numbers
            if message.get(34).decode() > str(self._inboundSeqNum):
                msg = self._clientMessage.sendResendRequest(self._inboundSeqNum, message.get(34))
                self.writeToOutputQueue(msg)
            # If input message is Session Message, process it.
            # Queue application messages.
            if not self._SessionMessageHandler(message):
                self.writeToInputQueue(message)
            self._inboundSeqNum = int(message.get(34).decode()) + 1
        except asyncio.CancelledError:
            logger.error("Client Read Message Timed Out")
            print("Client Read Message Timed Out")
    
    def sessionMessageHandler(self, message: simplefix.FixMessage) -> bool:
        """ Handle Session Message."""
        assert isinstance(message, simplefix.FixMessage)

        msgType = message.get(35)
        recvSeqNo = message.get(34)
        targetCompID = message.get(56)
        senderCompID = message.get(49)

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
                self.writeToOutputQueue(msg)
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
    




    def logon(self):
        threadName = f"FIX-Session-{self._senderCompID}"
        self.session_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        sessionThread = threading.Thread(target=self.main,daemon=True, name=threadName)
        sessionThread.start()
        
        msg = self._clientMessage.sendLogOn()
        self.writeToOutputQueue(msg)

    def main(self):
        self.session_loop.run_until_complete(self._sessionMain())
        # asyncio.run(self._sessionMain())

    async def _sessionMain(self):
        """ Creates Socket Connection and Runs Main Loop."""
        self._reader, self._writer = await asyncio.open_connection(self._host, self._port, loop=self.session_loop)
        # self._reader, self._writer = await asyncio.open_connection(self._host, self._port)
        self._connectionState = SocketConnectionState.CONNECTED
        logger.info(f"Socket Connection Open to {self._host}:{self._port}")
        print(f"Socket Connection Open to {self._host}:{self._port}")

        while self._connectionState != SocketConnectionState.LOGGED_OUT:
            print("StartLoop")
            if not self._writer_queue.empty():
                await self.sendMessage(self._writer_queue.get())
            if not self._reader_queue.empty():
                self._processApplicationMessage(self._reader_queue.get()) # Needs implementing. Does it have to be a different process/thread?
            if not self._logout:
                asyncio.ensure_future(self._readMessage())
            print("Continuing")
            if self.timeToSendHb():
                msg = self._clientMessage.sendHeartbeat()
                self.writeToOutputQueue(msg)

        logger.info("Exiting Main Session")
        print("Exiting Main Session")

    def ackTradeCaptureReport(self, message):
        msg = self._clientMessage.sendTradeCaptureReportAck(message.get(571))
        self.writeToOutputQueue(msg)

    def _processApplicationMessage(self, message: simplefix.FixMessage):
        assert isinstance(message, simplefix.FixMessage)

        FixEngine.printFix(message)
        msgType = message.get(35)

        if msgType == b'AE':
            self.ackTradeCaptureReport(message)


