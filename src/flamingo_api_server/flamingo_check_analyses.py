import os
import inspect
import logging
import threading

#import pandas as pd

from oasislmf.utils import (
    log,
    db,
    conf,
)

from flamingo_utils import * #do_poll_analysis
from flamingo_db_utils import * #get_stalled_analyses

CURRENT_DIRECTORY = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
INI_PATH = os.path.abspath(os.path.join(CURRENT_DIRECTORY, 'FlamingoApi.ini'))
CONFIG_PARSER = conf.load_ini_file(INI_PATH)
db.read_db_config(CONFIG_PARSER)



@log.oasis_log()
def check_analyses():
    analyses = get_stalled_analyses()
    for analysis in analyses:
        processrunid = analysis[0]
        analysis_status_location = analysis[1]
        #element_run_ids = list()
        element_run_ids = eval('{}'.format(analysis[2]))
        upload_directory = analysis[3]
        base_url = analysis[4]
        input_location = analysis[5]
    
        print(element_run_ids)
        logging.getLogger().info('processrunid: {},analysis_status_location: {},element_run_ids: {},upload_directory: {},base_url,input_location: {}'.format(
            processrunid,analysis_status_location,element_run_ids,upload_directory,base_url,input_location))

        #do_poll_analysis(processrunid,analysis_status_location,element_run_ids,upload_directory,base_url,input_location)
        target = do_poll_analysis
        args = (processrunid,analysis_status_location,element_run_ids,upload_directory,base_url,input_location,)
        new_thread = threading.Thread(target=target,args=args)
        new_thread.start()

