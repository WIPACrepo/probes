#!/usr/bin/python
import time
import logging

import classad
import htcondor

logger = logging.getLogger(__name__)

def get_defrag_info(pool, retry_delay=30, max_retries=4):
    coll = htcondor.Collector(pool)

    retries = 0

    data = {}
    
    while retries < max_retries:
        try:
            ads = coll.query(htcondor.AdTypes.Any, 'MyType=="Defrag"')
        except:
            #logger.error("trouble getting pool {0} ads, retrying in {1}s.".format(pool,retry_delay))
            retries += 1
            ads = None
            time.sleep(retry_delay)
        else:
            break

    if ads is None or len(ads) != 1:
        #logger.error("trouble retrieving pool {0} ads, giving up".format(pool))
        data = {}
        
    else:
        data = {
            "AvgDrainingUnclaimed": ads[0].get('AvgDrainingUnclaimed'),
            "WholeMachines": ads[0].get('WholeMachines'),
            "RecentDrainSuccesses": ads[0].get('RecentDrainSuccesses'),
            "MeanDrainedArrivalSD": ads[0].get('MeanDrainedArrivalSD'),
            "MeanDrainedArrival": ads[0].get('MeanDrainedArrival'),
            "RecentDrainFailures": ads[0].get('RecentDrainFailures'),
            "DrainedMachines": ads[0].get('DrainedMachines'),
            "MachinesDraining": ads[0].get('MachinesDraining'),
            "DrainSuccesses": ads[0].get('DrainSuccesses'),
            "WholeMachinesPeak": ads[0].get('WholeMachinesPeak'),
            "AvgDrainingBadput": ads[0].get('AvgDrainingBadput'),
        }
        
    return data
    
