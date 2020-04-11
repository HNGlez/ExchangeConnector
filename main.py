#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 29 09:03:41 2020

@author: hugonistalgonzalez
"""
from fixEngine import FixEngine

if __name__ == "__main__":
    fix = FixEngine('fixdc1.newrelease.erisx.com', '18038', 'testclient_HNDC', 'ErisX', 'testclient_HNDC', 'testclient_HNDC', 'FIX.4.4', )
    fix.logon()
    while True:
        pass