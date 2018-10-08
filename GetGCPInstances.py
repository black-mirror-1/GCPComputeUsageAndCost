import os,sys
sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)),'libs'))
import googleapiclient.discovery
import configparser
from google.oauth2 import service_account
import logging.handlers
import json
import GcpUtil
import Config

#Read Configs
#Read Configs and set logging
log_level = Config.getLogLevel()
projects = Config.getProjects()
log_path = Config.getLogPath()

#logging handler
log = logging.getLogger('gcp_instance')
if not log.handlers:
    # handler = logging.handlers.TimedRotatingFileHandler(filename=os.environ.get("LOGFILE", log_path), when='midnight', interval=1, backupCount=7)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s %(message)s")
    handler.setFormatter(formatter)
    log.addHandler(handler)
log.setLevel(log_level)

#Instanciate GcpUtil
gcputil = GcpUtil.GcpUtil()

try:
        instanceList=[]
        for project in projects:
            # Get disk info for all the projects
            gcputil.getInstanceAggregatedList(project,instanceList)
            #Get Monitoring data for the instances
            metrics=gcputil.getInstanceMetrics(project,300)

            # Correlate InstanceList and Metrics
            for instance in instanceList:
                disks = []
                networkInterfaces = []
                if 'disks' in instance:
                    for disk in instance['disks']:
                        disks.append(disk['source'][disk['source'].rindex('/') + 1:])
                if 'networkInterfaces' in instance:
                    for networkInterface in instance['networkInterfaces']:
                        networkInterfaces.append(networkInterface['networkIP'])
                for metric in metrics:
                    if instance['name'] == metric['labels']['instance_name']:
                        instance.update(metric['measures'])
                    continue
                log.info('kind=%s name=%s machineType=%s status=%s networkIPs=%s disks=%s zone=%s uptime=%s cpu_utilization=%s reserved_cores=%s' % (instance['kind'], instance['name'], instance['machineType'][instance['machineType'].rindex('/')+1:], instance['status'], networkInterfaces, disks, instance['zone'][instance['zone'].rindex('/')+1:], instance['uptime'], instance['utilization'], instance['reserved_cores']))

except Exception:
        import traceback
        log.error('Error : ' + traceback.format_exc())

# result = compute.disks().list(project=project, zone=zone).execute()
# print(result['items'])



# client = monitoring_v3.MetricServiceClient(credentials=credentials)
# project='dazzling-skill-210617'
# project_name = client.project_path(project)
#
# interval = monitoring_v3.types.TimeInterval()
# now = time.time()
# interval.end_time.seconds = int(now)
# interval.end_time.nanos = int(
#     (now - interval.end_time.seconds) * 10**9)
# interval.start_time.seconds = int(now - 300)
# interval.start_time.nanos = interval.end_time.nanos
# results = client.list_time_series(
#     project_name,
#     'metric.type = "compute.googleapis.com/instance/cpu/utilization"',
#     interval,
#     monitoring_v3.enums.ListTimeSeriesRequest.TimeSeriesView.HEADERS)
# for result in results:
#     print(result)



