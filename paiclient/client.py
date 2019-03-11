from hdfs import InsecureClient
from ipaddress import ip_address


__default_port_table__ = {
    'webhdfs': 50070,
}


class PAI:
    """this is the OpenPAI client (sdk)
    """

    def __init__(self, user: str, ip: str, ports: dict=__default_port_table__):
        "initialize the connection information"
        self.ip = ip_address(ip).exploded
        self.ports = dict(__default_port_table__)
        self.ports.update(ports)
        self.storage = InsecureClient('hdfs://%s:%d' % (self.ip, self.ports['webhdfs']))