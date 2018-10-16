import os,sys
sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)),'libs'))
import Config
import GcpUtil
import logging.handlers
import json

#Read Configs and set logging
log_level = Config.getLogLevel()
projects = Config.getProjects()
log_path = Config.getLogPath()
log = logging.getLogger('gcp_disk')
if not log.handlers:
    # handler = logging.handlers.TimedRotatingFileHandler(filename=os.environ.get("LOGFILE", log_path), when='midnight', interval=1, backupCount=7)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s %(message)s")
    handler.setFormatter(formatter)
    log.addHandler(handler)
log.setLevel(log_level)

gcputil = GcpUtil.GcpUtil()

try:
        diskList=[]
        for project in projects:
            # Get disk info for all the projects
            gcputil.getDiskAggregatedList(project,diskList)
            #Get Monitoring data for the disks
            metrics=gcputil.getDiskMetrics(project, 300)
            # print(metrics)


        #Correlate DiskList and Metrics
        for disk in diskList:
            users=[]
            if 'users' in disk:
                for user in disk['users']:
                    users.append(user[user.rindex('/') + 1:])
            # Add zero for metrics in case they are unavailable
            disk.update({
                "write_ops_count": 0.0,
                "read_ops_count":0.0,
                "write_bytes_count":0.0,
                "read_bytes_count":0.0,
            })
            for metric in metrics:
                if disk['name'] == metric['labels']['device_name']:
                    disk.update(metric['measures'])
                # print json.dumps(metric)
                continue 
            log.info('kind=%s name=%s sizeGB=%s status=%s type=%s users=%s zone=%s read_bytes_count=%s read_ops_count=%s write_bytes_count=%s write_ops_count=%s' % (disk['kind'], disk['name'], disk['sizeGb'], disk['status'], disk['type'][disk['type'].rindex('/')+1:], json.dumps(users), disk['zone'][disk['zone'].rindex('/')+1:],disk['read_bytes_count'],disk['read_ops_count'],disk['write_bytes_count'], disk['write_ops_count']))
        # print json.dumps(diskList)

except Exception:
        import traceback
        log.error('Error : ' + traceback.format_exc())



