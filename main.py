#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 29 09:03:41 2020

@author: hugonistalgonzalez
"""
from fixEngine import FixEngine, FIXClient
import asyncio

ENGINE = None
async def onMessage(message):
    FixEngine.printFix(message)

async def startMain(host, port, senderCompID, targetCompID, username, password, listener, loop):
    global ENGINE
    ENGINE = FIXClient(host, port, senderCompID, targetCompID, username, password, listener)
    await ENGINE.startClient(host, port, loop)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(startMain('fixmd.iris-x.net', '18037', 'testclient_HNMD', 'ErisX', 'testclient_HNMD', 'testclient_HNMD', 'FIX.4.4', loop))
    loop.run_forever()