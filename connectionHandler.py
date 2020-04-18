#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 11 09:31:25 2020

@author: hugo.nistal.gonzalez
"""

import asyncio
import sys
import simplefix
import logging
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
    def __init__(self, senderCompID, targetCompID, reader, writer, messageListener, fixVersion="FIX.4.4"):
        self._connectionState = SocketConnectionState.CONNECTED
        self._senderCompID = senderCompID
        self._reader = reader
        self._writer = writer
        self._sequenceNum = 0
        self.fixParser = simplefix.FixParser()
        self._fixProtocol = fixVersion
        self._session = FIXSessionHandler(targetCompID, senderCompID)
        self._clientMessage = None
        self._listener = messageListener
        asyncio.ensure_future(self.readMessage())

    async def disconnect(self):
        """ Disconnect Session."""
        await self.handleClose()

    async def handleClose(self):
        """ Handle Close Writer Socket Connection."""
        if self._connectionState == SocketConnectionState.DISCONNECTED:
            logger.info(f"{self._senderCompID} session -> DISCONNECTED")
            self._writer.close()
            self._connectionState = SocketConnectionState.DISCONNECTED
    
    async def sendMessage(self, message: simplefix.FixMessage):
        """ Send FIX Message to Server."""
        assert isinstance(message, simplefix.FixMessage)

        if self._connectionState != SocketConnectionState.CONNECTED and self._connectionState != SocketConnectionState.LOGGED_IN:
            return
        self._session.sequenceNumHandler(message)
        self._writer.write(message.encode())
        await self._writer.drain()

        logger.info(f"Client Sending Message: {FIXConnectionHandler.printFix(message.encode())}")
        print(f"Client Sending Message:   {FIXConnectionHandler.printFix(message.encode())}")
    
    async def readMessage(self):
        while True:
            try:
                buffer = await self._reader.read(4096)
                if not buffer:
                    raise ConnectionError

                self.fixParser.append_buffer(buffer)
                message = self.fixParser.get_message()
                if message is None:
                    continue
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
        beginString = message.get(8).decode()
        if beginString != self._fixProtocol:
            logger.warning(f"FIX Protocol is incorrect. Expected: {self._fixProtocol}; Received: {beginString}")
            await self.disconnect()
            return
        
        if not self.sessionMessageHandler(message):
            await self.messageNotification(message)
        await self.messageNotification(message)
        recvSeqNo = message.get(34).decode()

        seqNoState, expectedSeqNo = self._session.validateRecvSeqNo(recvSeqNo)
        if seqNoState == False:
            # Unexpected sequence number. Send resend request
            logger.info(f"Sending Resend Request of messages: {expectedSeqNo} to {recvSeqNo}")
            msg = self._clientMessage.sendResendRequest(expectedSeqNo, recvSeqNo)
            self.sendMessage(msg)
        else:
            self._session.updateRecvSeqNo(recvSeqNo)
    
    async def messageNotification(self, message):
        await self._listener(message)

    # Template functions to be overwritten in child class
    def sessionMessageHandler(self, message):
        return -1

    async def handleHearbeat(self):
        return -1 

    @staticmethod
    def printFix(msg):
        return msg.replace(b"\x01", b"|")
