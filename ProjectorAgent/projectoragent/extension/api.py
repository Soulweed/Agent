# -*- coding: utf-8 -*-
import time
import json
import requests
import socket
from struct import pack

import telnetlib


class API:
    def __init__(self, **kwargs):
        # Initialized common attributes
        self.variables = kwargs
        self.debug = True
        self.set_variable('offline_count', 0)
        self.set_variable('connection_renew_interval', 6000)

    def renewConnection(self):
        pass

    def set_variable(self, k, v):  # k=key, v=value
        self.variables[k] = v

    def get_variable(self, k):
        return self.variables.get(k, None)  # default of get_variable is none

    '''
    Attributes:
     ------------------------------------------------------------------------------------------
    label            GET          label in string
    status           GET          status
    unitTime         GET          time
    type             GET          type      
     ------------------------------------------------------------------------------------------
    '''

    '''
    API3 available methods:
    1. getDeviceStatus() GET
    2. setDeviceStatus() SET
    '''

    # ----------------------------------------------------------------------
    def setDeviceStatus(self, postmsg):
        setDeviceStatusResult = True

        if self.isPostMsgValid(postmsg) == True:  # check if the data is valid

            # _data = json.dumps(self.convertPostMsg(postmsg))
            # _data = _data.encode(encoding='utf_8')
            _data = str(self.convertPostMsg(postmsg))
            # print("DATA status = {}".format(_data))

            # open connection
            tn = telnetlib.Telnet(self.get_variable("ip"), self.get_variable("port"))
            print(tn)

            # send data
            key_buttom = ['power', 'home', "menu", "up", "down", "left", "right", "ok", "back", "vup", "vdown", "mute", "source", "info", "displaymode"]
            try:
                if _data in key_buttom:
                    send_mess = self.get_variable("command") + _data
                    print("send_mess: {}".format(send_mess))
                    print("Sending message...")
                    tn.write((send_mess + "\r").encode('ascii'))
                    tn.close()

            except:
                tn.close()
                print("ERROR: classAPI_Telnet_Projector connection failure! @ setDeviceStatus")
                setDeviceStatusResult = False
        else:
            print("The POST message is invalid, try again\n")

        return setDeviceStatusResult

    def isPostMsgValid(self, postmsg):  # check validity of postmsg
        dataValidity = True
        # TODO algo to check whether postmsg is valid
        return dataValidity

    def convertPostMsg(self, postmsg):
        msgToDevice = ""

        if ('status' in postmsg.keys()):
            msgToDevice = postmsg['status'].lower()
            self.set_variable('status', msgToDevice)
            print(msgToDevice)

        return msgToDevice

    # ----------------------------------------------------------------------


# This main method will not be executed when this class is used as a module
def main():
    # -------------Kittchen----------------
    projector = API(model='Optoma', api='API3', agent_id='25PROPT101001', types='projector', ip='192.168.10.11', port=81, command='projector#')

    # curtain.getDeviceStatus()
    projector.setDeviceStatus({"status": "menu"})
    time.sleep(3)


if __name__ == "__main__": main()
