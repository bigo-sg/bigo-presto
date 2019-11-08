# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import time
import urllib2
import random
import requests


def cluster_can_stop(coordinator):
    url = coordinator + "/v1/cluster"
    req = urllib2.Request(url)
    try:
        res_data = urllib2.urlopen(req)
        res = res_data.read()
        s = json.loads(res)
        if int(s['runningQueries']) == 0 and int(s['queuedQueries']) == 0:
            return True
    except urllib2.URLError:
        return True
    return False

def cluster_can_activate(coordinator, needed_workers):
    url = coordinator + "/v1/cluster"
    req = urllib2.Request(url)
    try:
        res_data = urllib2.urlopen(req)
        res = res_data.read()
        s = json.loads(res)
        if int(s['activeWorkers']) >= int(needed_workers):
            return True
    except urllib2.URLError:
        return False
    return False

def cluster_alive(coordinator):
    url = coordinator + "/v1/cluster"
    req = urllib2.Request(url)
    try:
        urllib2.urlopen(req)
    except urllib2.URLError:
        return False
    return True

def activate_cluster(gateway, name, auth):
    url = gateway + "/gateway/backend/activate/" + name
    import base64
    headers = {'Authorization': 'Basic ' + base64.b64encode(auth)}
    r = requests.post(url,headers=headers)
    if r.text == 'ok':
        return True
    return False

def deactivate_cluster(gateway, name, auth):
    url = gateway + "/gateway/backend/deactivate/" + name
    import base64
    headers = {'Authorization': 'Basic ' + base64.b64encode(auth)}
    r = requests.post(url,headers=headers)
    if r.text == 'ok':
        return True
    return False

def get_config(config_directory):
    fd = open(config_directory + '/presto_rolling.properties', 'rb')
    config = {}
    for line in fd:
        kv = line.strip('\n').split('=')
        if len(kv) == 2:
            config[kv[0]] = kv[1]
    fd.close()
    return config

def prepare_stop_worker(config_directory):
    print 'preparing stop...'
    other_config = get_config(config_directory)
    print other_config
    coordinator_host = other_config.get('coordinator_host')
    while not cluster_can_stop(coordinator_host):
        print 'check status of cluster_can_stop: false'
        time.sleep(random.randint(1, 5))


def prepare_stop_coord(config_directory):
    print 'preparing stop...'
    other_config = get_config(config_directory)
    print other_config
    gateway_host = other_config.get('gateway_host')
    coordinator_host = other_config.get('coordinator_host')
    presto_cluster_name = other_config.get('presto_cluster_name')
    gateway_auth = other_config.get('gateway_auth')
    print 'deactivating...'
    deactivate_cluster(gateway_host, presto_cluster_name, gateway_auth)
    while not cluster_can_stop(coordinator_host):
        print 'check status of cluster_can_stop: false'
        time.sleep(random.randint(1, 5))

def active_cluster(config_directory):
    other_config = get_config(config_directory)
    print other_config
    gateway_host = other_config.get('gateway_host')
    coordinator_host = other_config.get('coordinator_host')
    presto_cluster_name = other_config.get('presto_cluster_name')
    gateway_auth = other_config.get('gateway_auth')
    needed_workers = other_config.get('needed_workers')
    needed_activate = other_config.get('needed_activate')

    if needed_activate == 'false':
        print 'configed to not activate cluster'
        return
    while not cluster_can_activate(coordinator_host, needed_workers):
        print 'cluster_can_activate: false'
        time.sleep(1)
    print 'activating...'
    activate_cluster(gateway_host, presto_cluster_name, gateway_auth)
