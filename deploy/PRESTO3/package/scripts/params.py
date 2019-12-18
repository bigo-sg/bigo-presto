#!/usr/bin/env python
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

from resource_management.libraries.script.script import Script

# config object that holds the configurations declared in the config xml file

added_properties = ['event-listener.properties']

config = Script.get_config()


memory_configs = ['query.max-memory-per-node', 'query.max-memory']

java_home = '/data/opt/jdk/current/bin/java'


# add new presto cluster need to change the below params
etc_dir = '/etc/presto'

daemon_control_script = '/data/opt/presto/oms_current/bin/launcher  --etc-dir=/etc/presto3'

config_directory = '/etc/presto3'

all_content_key = 'all-other-configs.filecontent3'
config_properties_key = 'config.properties3'
connectors_properties_key = 'connectors.properties3'
jvm_config_key = 'jvm.config3'
node_properties_key = 'node.properties3'

node_data_dir = '/data1/var/presto/data3'

#end


pid_path = node_data_dir + '/var/run/launcher.pid'
configurations = {}
if config is not None and   config.has_key('configurations'):
    configurations = config['configurations']
node_properties = ''
jvm_config = ''
config_properties = ''
connectors_to_add = configurations[connectors_properties_key]['connectors.to.add']
connectors_to_delete = configurations[connectors_properties_key]['connectors.to.delete']
if configurations.has_key(node_properties_key):
    node_properties = configurations[node_properties_key]

if configurations.has_key(jvm_config_key):
    jvm_config = configurations[jvm_config_key]

if configurations.has_key(config_properties_key):
    config_properties = configurations[config_properties_key]

if configurations.has_key(connectors_properties_key):
    connectors_to_add = configurations[connectors_properties_key]['connectors.to.add']
    connectors_to_delete = configurations[connectors_properties_key]['connectors.to.delete']

