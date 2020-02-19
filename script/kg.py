# @file kg.py
# @brief startup and AP mode selection
# README: Startup code
# MODULE_ARCH:  
# CLASS_ARCH:
# GLOBAL USAGE: 
#standard
import getopt
import sys
import unittest

#extend
from configobj import ConfigObj

#library
import lib.globalclasses as gc
from lib.const import *
import codes.app as app
import codes.cli as cli
import codes.ut as ut
import codes.ui as ui


if __name__ =='__main__':
    # Read system parameters which are assigned while we launching "start.py".
    # If the input parameter is invalid, then display usage and return "command
    # line syntax" error code.
    apmode = AP_MODE_NORMAL

    #command line paramters handler
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ht", [])
        for opt, arg in opts:
            if opt == '-h':
                print ('sim.py [ -h ] [ -t ]')
                print ('    -h, --help: help message')
                print ('    -t, --test: unit test mode')
                sys.exit()
            elif opt in ("-t", "--test"):
                apmode = AP_MODE_UNITTEST
                print("Running as unittest mode!")
    except getopt.GetoptError:
        print ('usage: kg.py [ -h ] [ -t ]')       
        sys.exit(2) 
    
    

    #init global classes   
    gc.SETTING  = ConfigObj("include/kg.ini") 
    gc.UI = ui.UserInterface()
    gc.GAP = app.KApp()

    #run by different mode
    if apmode == AP_MODE_UNITTEST:
        suite = unittest.TestLoader().loadTestsFromTestCase(ut.UTGeneral)
        unittest.TextTestRunner(verbosity = 2).run(suite)
    else:
        
        cli.Cli().cmdloop()
    
