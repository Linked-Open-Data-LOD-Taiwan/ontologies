# @file cli.py
# @brief CLI of whole tool
# README: Command line interface
# MODULE_ARCH:  
# CLASS_ARCH:
# GLOBAL USAGE: 
#standard
import cmd
#extend
#library
import lib.globalclasses as gc
from lib.const import *
import codes.simple as simple

##### Code section #####
#Spec: local variable maintain, about, user commands, test commands
#How/NeedToKnow:
class Cli(cmd.Cmd):
    """Simple command processor example."""    
    def __init__(self,stdout=None):
        cmd.Cmd.__init__(self)
        self.prompt = 'WKG> '
        pass
############ cli maintain ####################        
    def do_set(self,line):
        """set scli variable, can be new or update.
        set var_name var_value
        ex: set a 123"""

        pars=line.split()
        if len(pars)==2:
            var = pars[0]
            value = pars[1]
        else:
            return 
        
        if var in ('dev_console_display','log_level_file','log_level_console'):
            value = int(value)
            
        gc.GAP.user_vars[var] = value
        # dynamic apply
        # these variable need to work out, log_level_file, log_level_console

    def do_show(self,line):
        """show simcli variables, if miss variable name, show all
        show variable_name
        system variables list:
            ;log level definition, DEBUG=10,INFO=20,WARNING=30,ERROR=40,CRITICAL=50
            log_level_console=20     #the console message log level
            log_level_file=40        #file message log level
            ;device console real time display
            dev_console_display=1    #(0) don't display (1) display
        ex: show dev_console_display """
        for var in gc.GAP.user_vars.keys():
            print("%s=%s" % ( var , gc.GAP.user_vars[var]))

    def do_simple(self, line):
        """simple test routine"""
        simple.river_comapre()
    
    def do_about(self, line):
        """About this software"""
        print("%s version: v%s" %(WKG_TITLE,WKG_VERSION))
    def do_quit(self, line):
        """quit"""
        return True
############ top command ####################                      
    #def do_test1(self, line):
    #    """current debug command"""
    #    self.cli_ebm.do_init("")
    def do_reset(self,line):
        """ reset for next run """
        
        gc.GAP.reset()
        print("reseted")
        
    def do_status(self,line):
        """ show current status """
        pass                   

