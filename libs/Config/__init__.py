import configparser
import os
import logging
import json

config = configparser.ConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..','..','conf', 'project.config'))

def getConfig():
    return config

def getLogLevel():
    log_level = config.get('Logs', 'log_level')
    if log_level == 'INFO':
        return logging.INFO
    elif log_level == 'DEBUG':
        return logging.DEBUG


def getLogPath():
    return config.get('Logs', 'log_path')


def getProjects():
    return json.loads(config.get('gcp', 'projects'))