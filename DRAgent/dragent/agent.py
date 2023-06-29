"""
Agent documentation goes here.
"""

__docformat__ = 'reStructuredText'

import logging
import sys
from volttron.platform.agent import utils
from volttron.platform.vip.agent import Agent, Core, RPC, PubSub
from volttron.platform.scheduling import periodic, cron
from pprint import pformat
import json
import socket
import time
from multiprocessing import Process
from Agent import settings
# import pyrebase
from datetime import datetime, timedelta
import asyncio
import os, time

_log = logging.getLogger(__name__)
utils.setup_logging()
__version__ = "0.1"

DEFAULT_MESSAGE = 'I am a DR Agent'
DEFAULT_AGENTID = "DRAgent"
DEFAULT_HEARTBEAT_PERIOD = 5

gateway_id = settings.gateway_id

# firebase config
# try:
#     config = {
#         "apiKey": settings.FIREBASE['apiKeyLight'],
#         "authDomain": settings.FIREBASE['authLight'],
#         "databaseURL": settings.FIREBASE['databaseLight'],
#         "storageBucket": settings.FIREBASE['storageLight']
#     }
#     firebase = pyrebase.initialize_app(config)
#     db =firebase.database()
#
# except Exception as er:
#     _log.debug(er)



class DRagent(Agent):

    def __init__(self, config_path,
                 **kwargs):
        super().__init__(**kwargs)

        self.config = utils.load_config(config_path)
        self._agent_id = self.config.get('agentid', DEFAULT_AGENTID)
        self._message = self.config.get('message', DEFAULT_MESSAGE)
        self._heartbeat_period = self.config.get('heartbeat_period',
                                                 DEFAULT_HEARTBEAT_PERIOD)

        self._drlist_path = self.config.get('drlistpath')
        self._drlogs_path = self.config.get('drlogspath')
        self._drhistory_path = self.config.get('drhistorypath')

        self._scenelist_path = self.config.get('scenelistpath')
        self.sceneconf = json.load(open(os.environ['VOLTTRON_ROOT'] + self._scenelist_path))

        _log.info(">>> : Found {} Scene Control List".format(len(self.sceneconf.get('scenelist'))))

        self.scenelist = self.sceneconf.get('scenelist')
        self.sceneid = [scene.get('scene_id') for scene in self.scenelist]
        _log.info(">>> : Scene ID : {}".format(self.sceneid))

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

    async def sendCommand(self, data):

        pub_topic = data.get('topic')
        pub_body = data.get('body')
        pid = data.get('id')

        _log.debug(msg="MultiProcess : {}".format(pid))
        _log.debug("TOPIC PUB : {}".format(pub_topic))
        _log.debug("BODY : {}".format(pub_body))
        _log.debug("----------------------------------------------")

        self.vip.pubsub.publish('pubsub', pub_topic,
                                message=pub_body
                                )

        _log.info("Published")


    @PubSub.subscribe('pubsub', "web/control/demand")
    def on_match_sendcommand(self, peer, sender, bus,  topic, headers, message):
        _log.info(msg="Matched Topic from Listener")
        _log.info(msg=message)

        # read JSON file
        with open(os.environ['VOLTTRON_ROOT'] + self._drlist_path) as fp:
            listObj = json.load(fp)
        fp.close()

        listObj.append(message)

        # write dr command
        with open(os.environ['VOLTTRON_ROOT'] + self._drlist_path, 'w') as json_file:
            json.dump(listObj, json_file, indent=4, separators=(',', ': '))
        json_file.close()


    @Core.schedule(cron('0,15,30,45 * * * *'))
    def check_start(self):
        # _log.info(msg="DR START")
        dt_now = datetime.now()
        # _log.info(msg=dt_now)

        # read drlist.json
        with open(os.environ['VOLTTRON_ROOT'] + self._drlist_path) as fp:
            listDR = json.load(fp)
        fp.close()

        for tmp in listDR:
            diff = abs((dt_now - datetime.strptime(tmp["start_dt"], "%d/%m/%y, %H:%M")).total_seconds())

            if diff < 100:
                # remove from listDR
                listDR.remove(tmp)

                # read drlogs.json
                with open(os.environ['VOLTTRON_ROOT'] + self._drlogs_path) as fp:
                    logsDR = json.load(fp)
                fp.close()

                logsDR.append(tmp)

                # write to drlogs.json
                with open(os.environ['VOLTTRON_ROOT'] + self._drlogs_path, 'w') as drlogs_json_file:
                    json.dump(logsDR, drlogs_json_file, indent=4, separators=(',', ': '))
                drlogs_json_file.close()

                # pub topic dr_on
                _topic = self.sceneconf.get('topic')
                sceneid = "fl1_scene025"
                scenecontrol = []

                if sceneid in self.sceneid:
                    _log.debug(" >>> :Scene ID matched")
                    for i in self.scenelist:
                        _log.info(msg=i)

                        if sceneid == i.get('scene_id'):
                            _log.info(">>> {a} == {b}".format(a=sceneid, b=i.get('scene_id')))
                            # - do stuff control Device
                            scenecontrol = i.get('scenecontrol')
                            _log.debug("GET SCENE CONTROL : {}".format(scenecontrol))

                            break

                    i = 0
                    for device in scenecontrol:

                        pub_topic = _topic + (device.get('device_type')).lower()
                        pub_body = device.get('device_control')

                        data = {"topic": pub_topic, "body": pub_body, "id": i}
                        time.sleep(1)
                        # self.sendCommand(data=data)

                        # asyncio.run(self.sendCommand(device))
                        _log.debug(msg="Data to Pub : {}".format(data))
                        loop = asyncio.get_event_loop()
                        loop.run_until_complete(self.sendCommand(data))

                # write to drlist.json
                with open(os.environ['VOLTTRON_ROOT'] + self._drlist_path, 'w') as drlist_json_file:
                    json.dump(listDR, drlist_json_file, indent=4, separators=(',', ': '))
                drlist_json_file.close()

    @Core.schedule(cron('0,15,30,45 * * * *'))
    def check_end(self):
        # _log.info(msg="DR STOP")
        dt_now = datetime.now()
        # _log.info(msg=dt_now)

        # read drlogs.json
        with open(os.environ['VOLTTRON_ROOT'] + self._drlogs_path) as fp:
            logsDR = json.load(fp)
        fp.close()

        for tmp in logsDR:
            diff = abs((dt_now - datetime.strptime(tmp["end_dt"], "%d/%m/%y, %H:%M")).total_seconds())

            if diff < 100:
                # remove from listDR
                logsDR.remove(tmp)

                # read drhistory.json
                with open(os.environ['VOLTTRON_ROOT'] + self._drhistory_path) as fp:
                    historyDR = json.load(fp)
                fp.close()

                historyDR.append(tmp)

                # write to drhistory.json
                with open(os.environ['VOLTTRON_ROOT'] + self._drhistory_path, 'w') as drhistory_json_file:
                    json.dump(historyDR, drhistory_json_file, indent=4, separators=(',', ': '))
                drhistory_json_file.close()

                # pub topic dr_off
                _topic = self.sceneconf.get('topic')
                sceneid = "fl1_scene026"
                scenecontrol = []

                if sceneid in self.sceneid:
                    _log.debug(" >>> :Scene ID matched")
                    for i in self.scenelist:
                        _log.info(msg=i)

                        if sceneid == i.get('scene_id'):
                            _log.info(">>> {a} == {b}".format(a=sceneid, b=i.get('scene_id')))
                            # - do stuff control Device
                            scenecontrol = i.get('scenecontrol')
                            _log.debug("GET SCENE CONTROL : {}".format(scenecontrol))

                            break

                    i = 0
                    for device in scenecontrol:
                        pub_topic = _topic + (device.get('device_type')).lower()
                        pub_body = device.get('device_control')

                        data = {"topic": pub_topic, "body": pub_body, "id": i}
                        time.sleep(1)
                        # self.sendCommand(data=data)

                        # asyncio.run(self.sendCommand(device))
                        _log.debug(msg="Data to Pub : {}".format(data))
                        loop = asyncio.get_event_loop()
                        loop.run_until_complete(self.sendCommand(data))

                # remove event in drlist.json
                with open(os.environ['VOLTTRON_ROOT'] + self._drlogs_path, 'w') as drlogs_json_file:
                    json.dump(logsDR, drlogs_json_file, indent=4, separators=(',', ': '))
                drlogs_json_file.close()


def main():
    """Main method called to start the agent."""
    utils.vip_main(DRagent,
                   version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
