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
import configparser
from fixClientMessages import FixClientMessages
from connectionHandler import FIXConnectionHandler, SocketConnectionState


class FixEngine(FIXConnectionHandler):

    def __init__(self, config, reader, writer, messageListener):
        FIXConnectionHandler.__init__(self, config, reader, writer, messageListener)
        self._config = config
        self._logout = False
        self._engineLogger.info(f"Socket Connection Open to {config['SocketHost']}:{config['SocketPort']}")
        self.clientMessage = FixClientMessages(config['SenderCompID'], config['TargetCompID'], config['SenderPassword'], config['BeginString'], config.getint('HeartBeatInterval'))
        asyncio.ensure_future(self._handleEngine())
    
    def getConnectionState(self):
        return self._connectionState

    async def _sessionMessageHandler(self, message: simplefix.FixMessage) -> bool:
        """ Handle Session Message."""
        assert isinstance(message, simplefix.FixMessage)
        # NEED TO ADD HANDLING OF BUSINESS REJECTS

        msgType = message.get(simplefix.TAG_MSGTYPE)
        if msgType == simplefix.MSGTYPE_LOGON: # Handle logon
            if self._connectionState == SocketConnectionState.LOGGED_IN:
                self._engineLogger.warning(f"{self._config['SenderCompID']} already looged in -> Ignoring Login Request.")
            else:
                self._connectionState = SocketConnectionState.LOGGED_IN
                self._engineLogger.info(f"{self._config['SenderCompID']} session -> LOGON")
                self._config['HeartBeatInterval'] = str(message.get(simplefix.TAG_HEARTBTINT).decode())
                return True
        elif self._connectionState == SocketConnectionState.LOGGED_IN:
            if msgType == simplefix.MSGTYPE_TEST_REQUEST: # Send test heartbeat when requested
                msg = self.clientMessage.sendHeartbeat()
                msg.append_pair(simplefix.TAG_TESTREQID, message.get(simplefix.TAG_TESTREQID))
                await self.sendMessage(msg)
                return True
            elif msgType == simplefix.MSGTYPE_LOGOUT: # Handle Logout
                self._connectionState = SocketConnectionState.LOGGED_OUT
                self._engineLogger.info(f"{self._config['SenderCompID']} session -> LOGOUT")
                self.handleClose()
                return True
            elif msgType == simplefix.MSGTYPE_HEARTBEAT:
                msg = self.clientMessage.sendHeartbeat()
                msg.append_pair(simplefix.TAG_TESTREQID, message.get(simplefix.TAG_TESTREQID))
                await self.sendMessage(msg)
                return True
            elif message.get(simplefix.TAG_RESETSEQNUMFLAG) == simplefix.RESETSEQNUMFLAG_YES: # If ResetSeqNum = Y Then Reset sequence
                self._session.updateRecvSeqNo(message.get(simplefix.TAG_MSGSEQNUM))
                return True
            else:
                return False
        else:
            self._engineLogger.warning(f"Cannot process message. {self._config['SenderCompID']} is not logged in.")
            return False

    async def _handleEngine(self):
        await self.logon()

        while self._connectionState != SocketConnectionState.DISCONNECTED:
            await self.readMessage()
            await self.expectedHeartbeat(self._config.getint('HeartBeatInterval'))
            if self._connectionState == SocketConnectionState.LOGGED_OUT:
                await self.logon()
        print("Exiting")
        return

            
class FIXClient:
    def __init__(self, configFile, gateway, listener):
        self._config = self.loadConfig(configFile, gateway)
        self._reader = None
        self._writer = None
        self._client = None
        self._messageListener = listener

    async def startClient(self, loop):
        """ Creates Socket Connection and Runs Main Loop."""
        self._reader, self._writer = await asyncio.open_connection(self._config["SocketHost"], self._config["SocketPort"], loop=loop)
        self._connectionState = SocketConnectionState.CONNECTED
        self._client = FixEngine(self._config, self._reader, self._writer, self._messageListener)

    def loadConfig(self, filePath, gateway):
        parser = configparser.SafeConfigParser()
        parser.read(filePath)
        if parser.has_section(gateway):
            return parser[gateway]
        else:
            raise Exception(f"{gateway} section not found in configuration file {filePath}")

    def getClient(self):
        return self._client