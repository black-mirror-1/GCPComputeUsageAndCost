import os,sys
sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)),'libs'))
import googleapiclient.discovery
import configparser
from google.oauth2 import service_account
import logging.handlers
import json
import GcpUtil

#Set Credentials
SCOPES = ['https://www.googleapis.com/auth/compute','https://www.googleapis.com/auth/monitoring.read']
SERVICE_ACCOUNT_FILE = '/Users/sandeep/Downloads/dazzling-skill-210617-e093ff6cfb3d.json'
#SERVICE_ACCOUNT_FILE = '/Users/sandeep/Downloads/dazzling-skill-210617-e5f21a7a4842.json'
#SERVICE_ACCOUNT_FILE = '/Users/sandeep/Downloads/My Project-68927e50a46b.json'
#Read Configs
config = configparser.ConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)),'conf','project.config'))
log_level = config.get('Logs', 'log_level')
projects = json.loads(config.get('gcp', 'projects'))
if log_level == 'INFO':
        log_level = logging.INFO
elif log_level == 'DEBUG':
        log_level = logging.DEBUG
#logging handler
log_path = config.get('Logs', 'log_path')
log = logging.getLogger('gcp_instance')
if not log.handlers:
    handler = logging.handlers.TimedRotatingFileHandler(filename=os.environ.get("LOGFILE", log_path), when='midnight', interval=1, backupCount=7)
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s %(message)s")
    handler.setFormatter(formatter)
    log.addHandler(handler)
log.setLevel(log_level)

#Instanciate GcpUtil
gcputil = GcpUtil.GcpUtil()

try:
        credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        all_instances=[]
        for project in projects:
                # Get disk info for all the projects
                service = googleapiclient.discovery.build('compute', 'v1', credentials=credentials)
                request = service.instances().aggregatedList(project=project)
                while request is not None:
                    response = request.execute()
                    # pprint(response)
                    for name, instances_scoped_list in response['items'].items():
                        # pprint((name, disks_scoped_list))
                        if 'warning' in instances_scoped_list:
                                log.debug(instances_scoped_list['warning']['message'])
                        else:
                                for instance in instances_scoped_list['instances']:
                                    all_instances.append(instance)
                                        # log.info('kind=%s name=%s sizeGB=%s status=%s type=%s users=%s zone=%s' % (disk['kind'], disk['name'], disk['sizeGb'], disk['status'], disk['type'][disk['type'].rindex('/')+1:], disk['users'], disk['zone'][disk['zone'].rindex('/')+1:]))
                    request = service.instances().aggregatedList_next(previous_request=request, previous_response=response)
                #Get Monitoring data for the instances
                gcputil.getInstanceMetrics(project,300)

        for instance in all_instances:
            # log.info(instance)
            disks=[]
            networkInterfaces=[]
            for disk in instance['disks']:
                disks.append(disk['source'][disk['source'].rindex('/')+1:])
            for networkInterface in instance['networkInterfaces']:
                networkInterfaces.append(networkInterface['networkIP'])
            log.info('kind=%s name=%s machineType=%s status=%s networkIPs=%s disks=%s zone=%s' % (instance['kind'], instance['name'], instance['machineType'][instance['machineType'].rindex('/')+1:], instance['status'], networkInterfaces, disks, instance['zone'][instance['zone'].rindex('/')+1:]))

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



