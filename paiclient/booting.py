import os
import re
import importlib
from hdfs import InsecureClient
from functools import partial    


# common
def getobj(name: str):
    mod_name, func_name = name.rsplit('.',1)
    mod = importlib.import_module(mod_name)
    return getattr(mod, func_name)


def hello_world():
    print('hello, world!')


def run_commands(commands: list):
    pass


# commands for hdfs operations
def get_ip(s: str):
    "get the ipv4 string from the input str"
    return re.compile ('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}').search(s).group()


def get_hdfs_client(ip: str=None, port: int=50070, user: str=None, **kwargs):
    "get hdfs client, use environment variables if not specified"
    ip = get_ip(os.envron['PAI_DEFAULT_FS_URI']) if ip is None else ip
    user = os.environ['PAI_USER_NAME'] if user is None else user
    return InsecureClient('http://%s:%d' % (ip, port), user=user, **kwargs)


def hdfs_transfer(hdfs_path: str, local_path: str, trans: str='download', client_args: dict=None, trans_args: dict=None):
    assert trans in ['download', 'upload'], 'unsupported transfer type {}'.format(trans)
    if trans == 'upload':
        assert os.path.exists(local_path), 'file to upload not found {}'.format(local_path)
    if trans == 'download' and not os.path.exists(local_path):
        if os.path.basename(hdfs_path) == os.path.basename(local_path):
            os.makedirs(os.path.basename(local_path), exist_ok=True)
        else:
            os.makedirs(local_path, exist_ok=True)
    client = get_hdfs_client(**(dict() if client_args is None else client_args))
    func = partial(getattr(client, trans), **(dict() if trans_args is None else trans_args))
    return func(hdfs_path, local_path)


hdfs_download = partial(hdfs_transfer, trans='download')


# entry point of boot loader
def bootloader(bootstraps: list):
    for b in bootstraps:
        assert isinstance(b, dict) and 'bootloader' in b and 'args' in b, 'bootloader not found: {}'.format(bootloader)
        getobj(b['bootloader'])(**b['args'])
        

if __name__ == '__main__':
    os.system('pwd')