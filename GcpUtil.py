import os,sys
from google.cloud import monitoring_v3
import time
from google.oauth2 import service_account
import googleapiclient.discovery
import json
import logging.handlers

class GcpUtil:
    def __init__(self):
        # self.config = configparser.ConfigParser()
        # self.config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'conf', 'project.config'))
        SCOPES = ['https://www.googleapis.com/auth/compute', 'https://www.googleapis.com/auth/monitoring.read']
        SERVICE_ACCOUNT_FILE = "/Users/sandeep/Downloads/dazzling-skill-210617-e093ff6cfb3d.json"
        self.credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)

        # Read Configs and set logging
        log_level = logging.INFO
        # projects = json.loads(self.config.get('gcp', 'projects'))
        self.log = logging.getLogger('gcp_disk')
        if not self.log.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s %(message)s")
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
        self.log.setLevel(log_level)

    def getDiskAggregatedList(self,project,diskList=[]):
        # Get disk info for all the projects
        service = googleapiclient.discovery.build('compute', 'v1', credentials=self.credentials)
        request = service.disks().aggregatedList(project=project)
        while request is not None:
            response = request.execute()
            # pprint(response)
            for name, disks_scoped_list in response['items'].items():
                # pprint((name, disks_scoped_list))
                if 'warning' in disks_scoped_list:
                    self.log.debug(disks_scoped_list['warning']['message'])
                else:
                    for disk in disks_scoped_list['disks']:
                        diskList.append(disk)
                        # log.info('kind=%s name=%s sizeGB=%s status=%s type=%s users=%s zone=%s' % (disk['kind'], disk['name'], disk['sizeGb'], disk['status'], disk['type'][disk['type'].rindex('/')+1:], disk['users'], disk['zone'][disk['zone'].rindex('/')+1:]))
            request = service.disks().aggregatedList_next(previous_request=request, previous_response=response)

    def getInstanceAggregatedList(self,project,instanceList=[]):
        service = googleapiclient.discovery.build('compute', 'v1', credentials=self.credentials)
        request = service.instances().aggregatedList(project=project,filter='status=RUNNING')
        while request is not None:
            response = request.execute()
            # pprint(response)
            for name, instances_scoped_list in response['items'].items():
                # pprint((name, disks_scoped_list))
                if 'warning' in instances_scoped_list:
                    self.log.debug(instances_scoped_list['warning']['message'])
                else:
                    for instance in instances_scoped_list['instances']:
                        instanceList.append(instance)
                        # log.info('kind=%s name=%s sizeGB=%s status=%s type=%s users=%s zone=%s' % (disk['kind'], disk['name'], disk['sizeGb'], disk['status'], disk['type'][disk['type'].rindex('/')+1:], disk['users'], disk['zone'][disk['zone'].rindex('/')+1:]))
            request = service.instances().aggregatedList_next(previous_request=request, previous_response=response)

    def getDiskMetrics(self, project, intervalMin):
        metric_types=["instance/disk/read_bytes_count", "instance/disk/read_ops_count", "instance/disk/write_bytes_count", "instance/disk/write_ops_count"]
        metrics=[]
        for metric_type in metric_types:
            self.pullMetricsFromGCP(project=project,intervalMin=intervalMin,metric_type=metric_type,aligner=monitoring_v3.enums.Aggregation.Aligner.ALIGN_MEAN,metrics=metrics)
        print json.dumps(metrics)
        return metrics

    def getInstanceMetrics(self, project, intervalMin):
        metric_types=["instance/cpu/utilization", "instance/uptime", "instance/cpu/reserved_cores"]
        metrics=[]
        for metric_type in metric_types:
            self.pullMetricsFromGCP(project=project,intervalMin=intervalMin,metric_type=metric_type,aligner=monitoring_v3.enums.Aggregation.Aligner.ALIGN_MEAN,metrics=metrics)
        return metrics

    def pullMetricsFromGCP(self,project, intervalMin,metric_type="instance/disk/read_ops_count", aligner=monitoring_v3.enums.Aggregation.Aligner.ALIGN_MEAN,metrics=[]):
        #define Aggregation
        aggregation = monitoring_v3.types.Aggregation()
        aggregation.alignment_period.seconds = intervalMin #5Min aggregation
        # Set the Aggregation
        aggregation.per_series_aligner = aligner
        #Define Interval
        interval = monitoring_v3.types.TimeInterval()
        now = time.time()
        interval.end_time.seconds = int(now)
        interval.end_time.nanos = int(
            (now - interval.end_time.seconds) * 10 ** 9)
        interval.start_time.seconds = int(now - intervalMin) #Last 5Min
        interval.start_time.nanos = interval.end_time.nanos
        #Get the Monitoring Service
        service = self.getMonitoringService()
        project_name = service.project_path(project)
        results = service.list_time_series(
            project_name,
            'metric.type = "compute.googleapis.com/' + str(metric_type)+ '"',
            interval,
            monitoring_v3.enums.ListTimeSeriesRequest.TimeSeriesView.FULL,
            aggregation)

        for result in results:
                latestPoint=None
                for point in result.points:
                    value_type = point.value.WhichOneof('value')
                    # data.append(point.__getattribute__(value_type))
                    latestPoint = point.value.__getattribute__(value_type)
                # labels = _dataframe._extract_labels(result)
                temp_metric={'labels':{},'measures':{}}
                temp_metric['labels']['resource_type'] = result.resource.type
                temp_metric['labels'].update(result.resource.labels)
                temp_metric['labels'].update(result.metric.labels)
                temp_metric['measures'][metric_type[metric_type.rindex('/') + 1:]] = latestPoint
                if metrics:
                    foundExistingMetric = False
                    for metric in metrics:
                        if 'device_name' in metric['labels'].keys():
                            if metric['labels']['device_name'] == temp_metric['labels']['device_name']:
                                metric['measures'][metric_type[metric_type.rindex('/') + 1:]] = latestPoint
                                foundExistingMetric = True
                                break
                        elif 'instance_id' in metric['labels'].keys():
                            if metric['labels']['instance_id'] == temp_metric['labels']['instance_id']:
                                metric['measures'][metric_type[metric_type.rindex('/') + 1:]] = latestPoint
                    if not foundExistingMetric:
                        metrics.append(temp_metric)
                else:
                    metrics.append(temp_metric)


    def getComputeService(self):
        service = googleapiclient.discovery.build('compute', 'v1', credentials=self.credentials)
        return service

    def getMonitoringService(self):
        service = monitoring_v3.MetricServiceClient(credentials=self.credentials)
        return service


