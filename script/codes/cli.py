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
from codes.opendata import *

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
        
        simple.opendata_colmap()
        #simple.river_comapre()
        #simple.opendata_get()
        #simple.opendata_getbynet()
        #simple.wikidata_get()
    def do_rivertree(self, line):
        """river tree trace
            rivertree [river_id] 
            river_id example: 151000_濁水溪,114000_淡水河
            ex: rivertree 0
        """
        river_id=0
        pars=line.split()
        if len(pars)==1:
            river_id = pars[0]
        
        simple.river_tree(river_id)  
    def do_dataset(self,line):
        """get dataset and display head
            dataset [dataset_id,] [dataset_id] ...
            ex: dataset 22228
        """
        dataset_id="22228"
        pars=line.split()
        if len(pars)==0:
            pars = [dataset_id]

        odMgr = OpenDataMgr()
        for par in pars:
            
            df = odMgr.get_dataset(int(par))
            #df = odMgr.od_df['df'][int(par)]
            if not df is None:
                
                print("--- dataset %s ---" %(par))
                print(odMgr.od_df.loc[int(par)])
                print(df.loc[0:5])
                print(df.columns)
                #print(df.describe())
                
    def do_test(self,line):
        """current test"""
        odMgr = OpenDataMgr()
        river_df = odMgr.get_riverlist()
        print(river_df.head())
        
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

