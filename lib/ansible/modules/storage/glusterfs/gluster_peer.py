#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright 2015 Nandaja Varma <nvarma@redhat.com>
# Copyright 2018 Red Hat, Inc.
#
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
#

from __future__ import absolute_import, division, print_function
__metaclass__ = type

ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = '''
---
module: gluster_peer
short_description: Attach/Detach peers to/from the cluster
description:
  - Create or diminish a GlusterFS trusted storage pool. A set of nodes can be
    added into an existing trusted storage pool or a new storage pool can be
    formed. Or, nodes can be removed from an existing trusted storage pool.
version_added: "2.6"
author: Sachidananda Urs (@sac)
options:
    state:
       choices: ["present", "absent"]
       default: "present"
       description:
          - Determines whether the nodes should be attached to the pool or
            removed from the pool. If the state is present, nodes will be
            attached to the pool. If state is absent, nodes will be detached
            from the pool.
       required: true
    nodes:
       description:
          - List of nodes that have to be probed into the pool.
       required: true
    master:
       description:
          - IP address or hostname of a node from which rest of the nodes are
            added into the pool. This option is specific to GlusterFS versions
            4.0 and above, GlusterD2 is used to add nodes.
       version_added: "2.7"
    user:
       description:
          - Username for REST API authentication.
       version_added: "2.7"
    passwd:
       description:
          - Password for REST API authentication.
       version_added: "2.7"
    verify:
       type: bool
       default: false
       description:
          - Enable/Disable certificate validation in case of https request.
       version_added: "2.7"
    force:
       type: bool
       default: "false"
       description:
          - Applicable only while removing the nodes from the pool. gluster
            will refuse to detach a node from the pool if any one of the node
            is down, in such cases force can be used.
requirements:
  - GlusterFS > 3.2
notes:
  - This module does not support check mode.
'''

EXAMPLES = '''
- name: Create a trusted storage pool
  gluster_peer:
        state: present
        nodes:
             - 10.0.1.5
             - 10.0.1.10

- name: Create a trusted storage pool (GD2)
  gluster_peer:
        state: present
        master: 10.70.41.250
        nodes:
           - 10.70.43.250
           - 10.70.41.22

- name: Delete a node from the trusted storage pool
  gluster_peer:
         state: absent
         nodes:
              - 10.0.1.10

- name: Delete a node from the trusted storage pool by force
  gluster_peer:
         state: absent
         nodes:
              - 10.0.0.1
         force: true

- name: Delete a node from the trusted storage pool (GD2)
  gluster_peer:
        state: absent
        master: 10.70.43.250
        nodes:
           - 10.70.41.250
'''

RETURN = '''
'''

from ansible.module_utils.basic import AnsibleModule
from distutils.version import LooseVersion

from ansible.module_utils.storage.glusterfs.peer import PeerApis
from ansible.module_utils.storage.glusterfs.exceptions import GlusterApiError


class Peer(object):
    def __init__(self, module):
        self.module = module
        self.state = self.module.params['state']
        self.nodes = self.module.params['nodes']
        self.glustercmd = self.module.get_bin_path('gluster', True)
        self.lang = dict(LANG='C', LC_ALL='C', LC_MESSAGES='C')
        self.action = ''
        self.force = ''
        # glusterd2 specific options
        self.user = self.module.params['user']
        self.passwd = self.module.params['passwd']
        self.verify = self.module.params['verify']
        self.port = self.module.params['port']
        self.master = self.module.params['master']

    def get_to_be_probed_hosts(self, hosts):
        peercmd = [self.glustercmd, 'pool', 'list']
        rc, output, err = self.module.run_command(peercmd,
                                                  environ_update=self.lang)
        peers_in_cluster = [line.split('\t')[1].strip() for
                            line in filter(None, output.split('\n')[1:])]
        try:
            peers_in_cluster.remove('localhost')
        except ValueError:
            # It is ok not to have localhost in list
            pass
        hosts_to_be_probed = [host for host in hosts if host not in
                              peers_in_cluster]
        return hosts_to_be_probed

    def call_peer_commands(self):
        result = {}
        result['msg'] = ''
        result['changed'] = False

        for node in self.nodes:
            peercmd = [self.glustercmd, 'peer', self.action, node]
            if self.force:
                peercmd.append(self.force)
            rc, out, err = self.module.run_command(peercmd,
                                                   environ_update=self.lang)
            if rc:
                result['rc'] = rc
                result['msg'] = err
                # Fail early, do not wait for the loop to finish
                self.module.fail_json(**result)
            else:
                if 'already in peer' in out or \
                   'localhost not needed' in out:
                    result['changed'] |= False
                else:
                    result['changed'] = True
        self.module.exit_json(**result)

    def gluster_peer_ops(self):
        if not self.nodes:
            self.module.fail_json(msg="nodes list cannot be empty")
        self.force = 'force' if self.module.params.get('force') else ''
        if self.state == 'present':
            self.nodes = self.get_to_be_probed_hosts(self.nodes)
            self.action = 'probe'
            # In case of peer probe, we do not need `force'
            self.force = ''
        else:
            self.action = 'detach'
        self.call_peer_commands()

    def glusterd2_add_peers(self):
        result = {}
        result['changed'] = False
        # Master cannot be null
        if not self.master:
            self.module.fail_json(msg="master has to be set for GlusterFS4.x" +
                                  " versions and above")
        master = "http://" + self.master + ":%s" % (self.port)
        client = PeerApis(master, self.user, self.passwd, self.verify)

        # Add the peers
        for peer in self.nodes:
            try:
                ret, result = client.peer_add(peer)
                if result:
                    result['changed'] = True
            except GlusterApiError as e:
                reason = e.message.reason
                # As of now, the API does not provide proper failure messages
                if reason.lower() == "conflict":
                    result['changed'] |= False
                elif reason.lower() == "internal server error":
                    self.module.fail_json(msg="Failed: %s" % reason)
        self.module.exit_json(**result)

    def _get_peer_id(self, client):
        peer_info = dict()

        try:
            status, peer_list = client.peer_status()
        except GlusterApiError as e:
            self.module.fail_json(msg="Unable to get peer list: %s" %
                                  e.message.reason)
        if status == 200:       # Success
            for peer in peer_list:
                hostname = peer['name']
                peer_ipaddr = peer['peer-addresses'][0].split(':')[0]
                peer_id = peer['id']
                peer_info[hostname] = peer_id
                peer_info[peer_ipaddr] = peer_id
        else:
            self.module.fail_json(msg="Failed to get peers")
        return peer_info

    def glusterd2_remove_peers(self):
        result = {}
        result['changed'] = False

        if not self.master:
            self.module.fail_json(msg="master variable has to be set for " +
                                  "GlusterFS-4.x versions and above")
        master = "http://" + self.master + ":%s" % (self.port)
        client = PeerApis(master, self.user, self.passwd, self.verify)

        # GlusterD2 accepts only peer ID for deletion
        peer_ids = self._get_peer_id(client)

        # Detach the peers listed in the playbook
        for node in self.nodes:
            try:
                ret, result = client.peer_remove(peer_ids[node])
                if ret == 204:  # Successful completion of the command
                    result['changed'] = True
            except GlusterApiError as e:
                self.module.fail_json(msg="Unable to delete peer: %s" %
                                      e.message.reason)
            except KeyError:
                result['changed'] |= False
        self.module.exit_json(**result)

    def glusterd2_peer_ops(self):
        if not self.nodes:
            self.module.fail_json(msg="nodes list cannot be empty")
        self.force = 'force' if self.module.params.get('force') else ''
        if self.state == 'present':
            self.glusterd2_add_peers()
        else:
            self.glusterd2_remove_peers()


def check_gluster_version(module):
    cmd = module.get_bin_path('gluster', True) + ' --version'
    lang = dict(LANG='C', LC_ALL='C', LC_MESSAGES='C')
    rc, output, err = module.run_command(cmd, environ_update=lang)
    if rc > 0:
        module.fail_json(msg="GlusterFS is not installed, GlusterFS" +
                         "version > 3.2 is required.")
    ver_line = output.split('\n')[0]
    version = ver_line.split(' ')[1]
    return version


def main():
    module = AnsibleModule(
        argument_spec=dict(
            force=dict(type='bool', required=False),
            master=dict(type='str', required=False),
            nodes=dict(type='list', required=True),
            state=dict(type='str', choices=['absent', 'present'],
                       default='present'),
            user=dict(type='str', required=False),
            passwd=dict(type='str', required=False, no_log=True),
            verify=dict(type='str', required=False, default=False),
            port=dict(type='str', required=False, default='24007'),
        ),
        supports_check_mode=False
    )
    pops = Peer(module)
    required_version = "3.2"
    # Verify if required GlusterFS version is installed
    version = check_gluster_version(module)
    if LooseVersion(version) < LooseVersion(required_version):
        module.fail_json(msg="GlusterFS version > %s is required" %
                         required_version)
    if LooseVersion(version) > LooseVersion("4"):
        pops.glusterd2_peer_ops()
    else:
        pops.gluster_peer_ops()


if __name__ == "__main__":
    main()
