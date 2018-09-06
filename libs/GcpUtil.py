import configparser
import os
from google.cloud import monitoring_v3
import time
from google.oauth2 import service_account
import googleapiclient.discovery
import json

class GcpUtil:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'conf', 'project.config'))
        SCOPES = ['https://www.googleapis.com/auth/compute', 'https://www.googleapis.com/auth/monitoring.read']
        SERVICE_ACCOUNT_FILE = '/Users/sandeep/Downloads/dazzling-skill-210617-e093ff6cfb3d.json'
        self.credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    def getDiskMetrics(self, project, intervalMin):
        metric_types=json.loads(self.config.get('gcp','diskMetrics'))
        metrics=[]
        for metric_type in metric_types:
            self.pullMetricsFromGCP(project=project,intervalMin=intervalMin,metric_type=metric_type,aligner=monitoring_v3.enums.Aggregation.Aligner.ALIGN_MEAN,metrics=metrics)
        print(metrics)

    def getInstanceMetrics(self, project, intervalMin):
        metric_types=json.loads(self.config.get('gcp','instanceMetrics'))
        metrics=[]
        for metric_type in metric_types:
            self.pullMetricsFromGCP(project=project,intervalMin=intervalMin,metric_type=metric_type,aligner=monitoring_v3.enums.Aggregation.Aligner.ALIGN_MEAN,metrics=metrics)
        print(metrics)

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
        # if other metrics exists, then just add fields to the existing metrics
        if metrics:
            # for result in results:
            for result in results:
                latestPoint = None
                for point in result.points:
                    value_type = point.value.WhichOneof('value')
                    # data.append(point.__getattribute__(value_type))
                    latestPoint = point.value.__getattribute__(value_type)
                # labels = _dataframe._extract_labels(result)
                labels = {'resource_type': result.resource.type, metric_type[metric_type.rindex('/') + 1:]: latestPoint}
                labels.update(result.resource.labels)
                labels.update(result.metric.labels)
                for metric in metrics:
                    if 'device_name' in metric.keys():
                        if metric['resource_type'] == labels['resource_type'] and metric['instance_id'] == labels['instance_id'] and metric['device_name'] == labels['device_name']:
                            metric[metric_type[metric_type.rindex('/') + 1:]] = latestPoint
                    else:
                        if metric['resource_type'] == labels['resource_type'] and metric['instance_id'] == labels['instance_id']:
                            metric[metric_type[metric_type.rindex('/') + 1:]] = latestPoint
                    continue
        else:
            for result in results:
                latestPoint=None
                for point in result.points:
                    value_type = point.value.WhichOneof('value')
                    # data.append(point.__getattribute__(value_type))
                    latestPoint = point.value.__getattribute__(value_type)
                # labels = _dataframe._extract_labels(result)
                labels = {'resource_type': result.resource.type}
                labels.update(result.resource.labels)
                labels.update(result.metric.labels)
                labels[metric_type[metric_type.rindex('/') + 1:]] = latestPoint
                metrics.append(labels)


    def getComputeService(self):
        service = googleapiclient.discovery.build('compute', 'v1', credentials=self.credentials)
        return service

    def getMonitoringService(self):
        service = monitoring_v3.MetricServiceClient(credentials=self.credentials)
        return service


