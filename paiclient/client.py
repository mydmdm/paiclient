import os
import re
from ipaddress import ip_address


__default_port_table__ = {
    'webhdfs': 50070,
}




class PAI:
    """this is the OpenPAI client (sdk)
    """

    def __init__(self, user: str, ip: str, ports: dict=__default_port_table__):
        "initialize the connection information"
        self.user = user
        self.ip = ip_address(ip).exploded
        self.ports = dict(__default_port_table__)
        self.ports.update(ports)
        self.storage = InsecureClient('http://%s:%d' % (self.ip, self.ports['webhdfs']), user=self.user)
        
    @staticmethod
    def from_envrons(**kwargs):
        return PAI(user=os.environ['PAI_USER_NAME'], ip=get_ip(os.environ['PAI_DEFAULT_FS_URI']), **kwargs)