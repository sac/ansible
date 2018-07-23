from ansible.module_utils.storage.glusterfs.peer import PeerApis
from ansible.module_utils.storage.glusterfs.volume import VolumeApis

class Client(VolumeApis, PeerApis):
    pass
