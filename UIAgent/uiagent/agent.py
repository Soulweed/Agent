"""
Agent documentation goes here.
"""

__docformat__ = 'reStructuredText'

import logging
import sys
from volttron.platform.agent import utils
from volttron.platform.vip.agent import Agent, Core, RPC, PubSub
from tkinter import *
import tkinter
import json


_log = logging.getLogger(__name__)
utils.setup_logging()
__version__ = "0.1"


DEFAULT_MESSAGE = 'I am a UI Agent'
DEFAULT_AGENTID = "uiagent"
DEFAULT_HEARTBEAT_PERIOD = 5

# class FrontEndClass():
class Uiagent(Agent):
    """
    Document agent constructor here.
    """
    def __init__(self, config_path,
                 **kwargs):
        super().__init__(**kwargs)
        self.config = utils.load_config(config_path)
        
        # self.current_conf = {} # Alway get current status.
        self.command_conf = {
                                "power": "ON",
                                "mode" : "COLD",
                                "fan": "FAN1",
                                "setpoint": 25
                                }
        
        log_level = self.config.get('log-level', 'INFO')
        if log_level == 'ERROR':
            self._logfn = _log.error
        elif log_level == 'WARN':
            self._logfn = _log.warn
        elif log_level == 'DEBUG':
            self._logfn = _log.debug
        else:
            self._logfn = _log.info
        
    def build_ui(self):
        
        self.new_state = None
        self.old_state = None
        
        self._mode = None
        self.temperature = 25 # It Default Temperature.
        self._control = None
        self._temp = None
        self.control_variable = None
        self.temp_variable = None
        self.mode_variable = None
        self.top = tkinter.Tk()
        # Code to add widgets will go here...
        self.top.geometry("480x320")
        
        self.mode_frame = tkinter.Frame(self.top)
        self.mode_frame.pack()
        self.mode_variable = IntVar()
        self.mode_variable.set(1)
        self.mode_variable = tkinter.StringVar(value="manual")
    
        self.eco_btn = tkinter.Radiobutton(self.mode_frame, text="ECO", variable=self.mode_variable, command=self.callBack_mode,
                            indicatoron=False, value="eco", width=18, height=5)
        self.eco_btn.grid(row=0, column=0)
        self.dr_btn = tkinter.Radiobutton(self.mode_frame, text="DR", variable=self.mode_variable, command=self.callBack_mode,
                            indicatoron=False, value="dr", width=18, height=5)
        self.dr_btn.grid(row=2, column=0)
        self.ma_btn = tkinter.Radiobutton(self.mode_frame, text="MANUAL", variable=self.mode_variable, command=self.callBack_mode,
                            indicatoron=False, value="manual", width=18, height=5)
        self.ma_btn.grid(row=4, column=0)
        
        self.mode_frame.place(x=15, y= 30)
        
        self.ok_btn = Button(self.top, text = "OK", command = self.doSendCommand, height = 4, width = 30)
        self.ok_btn.place(x = 185,y = 220)
        
    
        self.switch_frame = tkinter.Frame(self.top)
        self.switch_frame.pack()

        self.switch_variable = IntVar()
        self.switch_variable.set(1)
        # selection = var.get()

        self.switch_variable = tkinter.StringVar(value="medium")
        self.aut_button = tkinter.Radiobutton(self.switch_frame, text="Auto", variable=self.switch_variable,
                                    indicatoron=False, value="FANA", width=8, height=2)

        self.low_button = tkinter.Radiobutton(self.switch_frame, text="Low", variable=self.switch_variable,
                                    indicatoron=False, value="FAN1", width=8, height=2)

        self.med_button = tkinter.Radiobutton(self.switch_frame, text="Medium", variable=self.switch_variable,
                                    indicatoron=False, value="FAN2", width=8, height=2)

        self.high_button = tkinter.Radiobutton(self.switch_frame, text="High", variable=self.switch_variable,
                                    indicatoron=False, value="FAN3", width=8, height=2)

        self.aut_button.pack(side="left")
        self.low_button.pack(side="left")
        self.med_button.pack(side="left")
        self.high_button.pack(side="left")

        self.switch_frame.place(x=185, y=30)

        # -- UI Toggle Button group 2 --------------------------
        # ------------------------------------------------------
        self.control_frame = tkinter.Frame(self.top)
        self.control_frame.pack()
        self.control_variable = IntVar()
        # self.control_variable.set(1)
        # self.control_variable = tkinter.StringVar(value="on")
        self.on_button = tkinter.Radiobutton(self.control_frame, text="ON", variable=self.control_variable,
                                    indicatoron=False, value=1, width=8, height=2, command=self.callBack_control)

        self.off_button = tkinter.Radiobutton(self.control_frame, text="OFF", variable=self.control_variable,
                                    indicatoron=False, value=0, width=8, height=2, command=self.callBack_control)

        self.on_button.pack(side="left")
        self.off_button.pack(side="left")
        self.control_frame.place(x=185, y=80)

        # -- UI UP DOWN -----------------------------------
        # -------------------------------------------------
        self.temp_frame = tkinter.Frame(self.top)
        self.temp_frame.pack()
        self.temp_variable = IntVar()
        self.up_down_frame = tkinter.Frame(self.top)
        self.up_down_frame.pack()
        self.up_button = tkinter.Button(self.up_down_frame, 
                                            font=('calibri', 12),
                                            text="UP", 
                                            width=4, height=1,
                                            command=self.callBack_up)

        self.down_button = tkinter.Button(self.up_down_frame,
                                            font=('calibri', 12),
                                            text="DOWN",
                                            width=4, height=1,
                                            command=self.callBack_down)
    
        self.up_button.pack(side="left")
        self.down_button.pack(side="left")
        
        self.up_down_frame.place(x=325,y=83)
        self.temp_frame.place(x=325, y=80)
        
        # -- Label Temperature Widget.
        self.lbl = Label(self.top, font = ('calibri', 40, 'bold'), 
                            background = 'whitesmoke', 
                            foreground = 'springgreen2', text= str(float(self.temperature)),
                            width=7, height=1, justify='center'
                            ) 
    
        self.lbl.place(x=188, y=140)
        self.top.mainloop()
    
    def callBack_mode(self):
        self._mode = self.mode_variable.get()
        if self._mode != 'manual':
            self.ok_btn['state'] = DISABLED
            if self._mode == 'dr': # DR Mode for Enable Trigger Signal from Outside.
                print("DR Mode Actived")
                # TODO : Send msg to Agent Optimizer Here.
                self.vip.pubsub.publish('pubsub', "ui/mode/control", 
                                        message=json.dumps(
                                            {'control':'dr'})
                                        )
                                        
                
            elif self._mode == 'eco': # ECO Mode is Automation adjust Temperature.
                print("ECO Mode Activated")
                self.vip.pubsub.publish('pubsub', "ui/mode/control",
                                        message=json.dumps(
                                            {'control':'eco'})
                                        )
                
        else:
            self.ok_btn['state'] = NORMAL
            print("MANUAL Mode Activated")
            # TODO : Send msg to Agent Optimizer for stop all automatic task and wait command from USER Only.
            self.vip.pubsub.publish('pubsub', "ui/mode/control", 
                                    message=json.dumps(
                                        {'control':'manual'})
                                    )
            
            
    def callBack_control(self): # Function to handle ON/OFF A.C.
        self._control = self.control_variable.get()
        if self._control == 1:
            self.command_conf['power'] = "ON"
        else:
            self.command_conf['power'] = "OFF"
    
    def callBack_up(self): # Increase temperature for 1 step handle function
        self.temperature = self.temperature + 1
        self.lbl.config(text=str(float(self.temperature)))
        self.command_conf['setpoint'] = self.temperature
        
    def callBack_down(self): # Reduce temperature for 1 step handle function
        self.temperature = self.temperature - 1
        self.lbl.config(text=str(float(self.temperature)))
        self.command_conf['setpoint'] = self.temperature
    
    def doSendCommand(self): # OK Button Pressed handler function
        self.command_conf['fan'] = self.switch_variable.get()
        _log.info(msg="Command Configuration Send : {}".format(self.command_conf))
        conf = json.dumps(self.command_conf)
        self.vip.pubsub.publish('pubsub', topic="ui/command/conf", message=conf)
        _log.debug(msg="Publish Message : {}".format(conf))
        
                
    @Core.receiver("onstart")
    def onstart(self, sender, **kwargs):
        self._ui = self.build_ui()
        _log.info(msg="UI Agent Started.")
        
    @Core.receiver("onstop")
    def onstop(self, sender, **kwargs):
        """
        This method is called when the Agent is about to shutdown, but before it disconnects from
        the message bus.
        """
        _log.debug(msg="UI Agent is shuting down ...")
        self.build_ui.top.quit()

def main():
    """Main method called to start the agent."""
    utils.vip_main(Uiagent, 
                   version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
