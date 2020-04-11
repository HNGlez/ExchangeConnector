#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 11 09:31:25 2020

@author: hugo.nistal.gonzalez
"""

import asyncio
import sys
import simplefix
from fixEngine import SocketConnectionState
import logging
import queue
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(filename='logs/fix_logs.log', format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

class FIXConnectionHandler:
    def __init__(self, senderCompID, reader, writer, fixVersion="FIX.4.4"):
        self._connectionState = SocketConnectionState.CONNECTED
        self._senderCompID = senderCompID
        self._reader = reader
        self._writer = writer
        self._writerQueue = queue.Queue()
        self._readerQueue = queue.Queue()
        self._sequenceNum = 0
        self.fixParser = simplefix.FixParser()
        self._fixProtocol = fixVersion


    async def disconnect(self):
        """ Disconnect Session."""
        await self.handleClose()

    async def handleClose(self):
        """ Handle Close Writer Socket Connection."""
        if self._connectionState == SocketConnectionState.DISCONNECTED:
            logger.info(f"{self._senderCompID} session -> DISCONNECTED")
            self._writer.close()
            self._connectionState = SocketConnectionState.DISCONNECTED
    
    def writeToOutputQueue(self, message: simplefix.FixMessage):
        """ Write Message to Output Queue."""
        assert isinstance(message, simplefix.FixMessage)
        self._writerQueue.put(message)

    def writeToApplicationQueue(self, message: simplefix.FixMessage):
        """ Write Message to Input Queue."""
        assert isinstance(message, simplefix.FixMessage)
        self._readerQueue.put(message)
    
    async def sendMessage(self, message: simplefix.FixMessage):
        """ Send FIX Message to Server."""
        assert isinstance(message, simplefix.FixMessage)
        if self._connectionState != SocketConnectionState.CONNECTED and self._connectionState != SocketConnectionState.LOGGED_IN:
            return
        
        self._sequenceNumHandler(message)
        self._writer.write(message.encode())
        await self._writer.drain()

        logger.info(f"Client Sending Message {FIXConnectionHandler.printFix(message.encode())}")
        print(f"Client Sending Message {FIXConnectionHandler.printFix(message.encode())}")
    


    async def _readMessage(self):
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
                print(FIXConnectionHandler.printFix(message).decode('UTF-8'))
                assert isinstance(message, simplefix.FixMessage)
                await self.processMessage(message)
            except ConnectionError as e:
                logger.error("Connection Closed.", exc_info=True)
                print(e)
            except asyncio.CancelledError as e:
                logger.error(f"{self._senderCompID} Read Message Timed Out")
                print(e)
    
    async def processMessage(self, message: simplefix.FixMessage):
        beginString = message.get(8).decode()
        if beginString != self._fixProtocol:
            logger.warning(f"FIX Protocol is incorrect. Expected: {self._fixProtocol}; Received: {beginString}")
            await self.disconnect()
            return
        
        if not self.sessionMessageHandler(message):
            self.writeToApplicationQueue(message)
        
        recvSeqNo = message.get(34).decode()
        
    
        




                


    @staticmethod
    def printFix(msg):
        return msg.replace(b"\x01", b"|")
