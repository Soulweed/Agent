"""
Agent documentation goes here.
"""

__docformat__ = 'reStructuredText'

import logging
import sys
from volttron.platform.agent import utils
from volttron.platform.vip.agent import Agent, Core, RPC, PubSub
from volttron.platform.scheduling import periodic
from pprint import pformat
import json
import socket
from .extension import api
from multiprocessing import Process
from Agent import settings
import pyrebase
from datetime import datetime
import asyncio, concurrent.futures
import os
_log = logging.getLogger(__name__)
utils.setup_logging()
__version__ = "0.1"

DEFAULT_MESSAGE = 'I am a Weather Station Agent'
DEFAULT_AGENTID = "WeatherStationTelnetAgent"
DEFAULT_HEARTBEAT_PERIOD = 5

gateway_id = settings.gateway_id

# firebase config
try:
    config = {
        "apiKey": settings.FIREBASE['apiKeyLight'],
        "authDomain": settings.FIREBASE['authLight'],
        "databaseURL": settings.FIREBASE['databaseLight'],
        "storageBucket": settings.FIREBASE['storageLight']
    }
    firebase = pyrebase.initialize_app(config)
    db = firebase.database()

except Exception as er:
    _log.debug(er)


class Weatherstationtelnetagent(Agent):
    """
    Document agent constructor here.
    """

    # TODO -- Need Revise again
    async def getstatus_proc(self, devices):  # Function for Asyncronous

        # Devices is tuple index 0 is Devices ID , 1 is IPADDRESS
        _log.info(msg="Start Get Status from {}".format(devices[1]))
        loop = asyncio.get_event_loop()

        def getstatus_task(devices):


            try:
                weatherstation = api.API(model='Wisco', api='API3', agent_id='27WIS010101', types='sensor',
                                         ip=(devices[1])['ip'], port=(devices[1])['port'])

                weatherstation.getDeviceStatus()

                # TODO : Update Firebase with _status variable
                db.child(gateway_id).child('devicetype').child('weatherstation').child(devices[0]).child('DT').set(datetime.now().replace(microsecond=0).isoformat())
                db.child(gateway_id).child('devicetype').child('weatherstation').child(devices[0]).child('AMBIENTTEMP').set(weatherstation.variables['ambienttemp'])
                db.child(gateway_id).child('devicetype').child('weatherstation').child(devices[0]).child('IRRADIANCE').set(weatherstation.variables['radiation'])
                db.child(gateway_id).child('devicetype').child('weatherstation').child(devices[0]).child('WINDSPEED').set(weatherstation.variables['windspeed'])
                db.child(gateway_id).child('devicetype').child('weatherstation').child(devices[0]).child('WINDDIRECTION').set(weatherstation.variables['winddir'])

            except Exception as err:
                _log.error(err)
                pass

        try:

            loop.run_in_executor(None, getstatus_task, devices)
            # response1 = await future1
            loop.close()
            res = await loop

        except Exception as e:
            pass


    def __init__(self, config_path,
                 **kwargs):
        super().__init__(**kwargs)

        self.config = utils.load_config(config_path)
        self._agent_id = self.config.get('agentid', DEFAULT_AGENTID)
        self._message = self.config.get('message', DEFAULT_MESSAGE)
        self._heartbeat_period = self.config.get('heartbeat_period',
                                                 DEFAULT_HEARTBEAT_PERIOD)

        self.iplist_path = self.config.get('pathconf')
        # self.members = json.load(open(self.iplist_path))
        self.members = json.load(open(os.environ['VOLTTRON_ROOT']+self.iplist_path))
        # print(os.environ['VOLTTRON_ROOT'])

        _log.debug("IP List : {}".format(self.members))

        try:
            self._heartbeat_period = int(self._heartbeat_period)
        except:
            _log.warning('Invalid heartbeat period specified setting to default')
            self._heartbeat_period = DEFAULT_HEARTBEAT_PERIOD
        log_level = self.config.get('log-level', 'INFO')
        if log_level == 'ERROR':
            self._logfn = _log.error
        elif log_level == 'WARN':
            self._logfn = _log.warn
        elif log_level == 'DEBUG':
            self._logfn = _log.debug
        else:
            self._logfn = _log.info

    @Core.receiver("onstart")
    def onstart(self, sender, **kwargs):
        
        # TODO :  Start Server Listener Here

        pass

    @Core.schedule(periodic(60))
    def updatestatus(self):
        _log.info(msg="Get Current Status")
        procs = []

        for k, v in self.members.items():
            devices = (k, v)
            # proc = Process(target=self.getstatus_proc, args=(devices,))
            # procs.append(proc)
            # proc.start()

            #  --- Change Multiprocess to async function
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.getstatus_proc(devices=devices))

        # TODO : if you want to wait the process completed Uncomment code below
        # for proc in procs:
        #     proc.join()



def main():
    """Main method called to start the agent."""
    from gevent import monkey

    monkey.patch_all()

    utils.vip_main(Weatherstationtelnetagent,
                   version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
