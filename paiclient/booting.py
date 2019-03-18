import os
import sys
import re
import importlib
import shutil
import random
import simplejson as json
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
    os.system(' && '.join(commands))


def shuffle_text(text: str, seed=None, seed_env: str=None):
    "randomly shuffling a text string by given seed"
    if seed is None and seed_env is not None:
        seed = os.environ[seed_env]
    if seed is not None:
        random.seed(seed)
    s = list(text)
    random.shuffle(s)
    return ''.join(s)


def shffle_back_text(text: str, seed=None, seed_env: str=None):
    "inverse function of shuffle_text"
    if seed is None and seed_env is not None:
        seed = os.environ[seed_env]
    if seed is not None:
        random.seed(seed)
        
        
# commands for git
def git_config(user: str, email: str, keystr: str=None):
    run_commands([
        'git config --global user.name %s' % (user),
        'git config --global user.email %s' % (email),
    ])
    if keystr is not None:
        os.makedirs('~/.ssh', exist_ok=True)
        with open('~/.ssh/id_rsa', 'w') as fn:
            fn.write(key)
    return 'git configured'

# commands for hdfs operations
def get_ip(s: str):
    "get the ipv4 string from the input str"
    return re.compile ('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}').search(s).group()


def get_hdfs_client(ip: str=None, port: int=50070, user: str=None, **kwargs):
    "get hdfs client, use environment variables if not specified"
    ip = get_ip(os.environ['PAI_DEFAULT_FS_URI']) if ip is None else ip
    user = os.environ['PAI_USER_NAME'] if user is None else user
    return InsecureClient('http://%s:%d' % (ip, port), user=user, **kwargs)


def hdfs_upload(hdfs_path: str, local_path: str, client_args: dict=None, trans_args: dict=None):
    assert os.path.exists(local_path), 'file to upload not found {}'.format(local_path)
    client = get_hdfs_client(**(dict() if client_args is None else client_args))
    hdfs_transfer(client, hdfs_path, local_path, 'upload', **(dict() if trans_args is None else trans_args))

    
def hdfs_download(hdfs_path: str, local_path: str, client_args: dict={}, trans_args: dict={}, extract_in: str=None):
    "local path would be the destination folder or with the same basename with hdfs_path"
    if os.path.basename(hdfs_path) == os.path.basename(local_path):
        local_path = os.path.dirname(local_path)
    os.makedirs(local_path, exist_ok=True)
    client = get_hdfs_client(**client_args)
    status = client.status(hdfs_path)
    if status['type'].lower() == 'directory':
        raise NotImplementedError('directory downloading not supported yet: {}'.format(client.list(hdfs_path)))
    hdfs_file, filename, local_file = hdfs_path, os.path.basename(hdfs_path), os.path.join(local_path, os.path.basename(hdfs_path))
    if not os.path.exists(local_file) or trans_args.get('overwrite', False):
        hdfs_transfer(client, hdfs_file, local_file, 'download', **trans_args)
    if extract_in is not None:
        shutil.unpack_archive(local_file, extract_in)
    return 'hdfs file downloaded %s -> %s' %(hdfs_path, local_path)
    
def hdfs_transfer(client, hdfs_path: str, local_path: str, trans: str='download', **kwargs):
    assert trans in ['download', 'upload'], 'unsupported transfer type {}'.format(trans)
    func = partial(getattr(client, trans), **kwargs)
    return func(hdfs_path, local_path)


# entry point of boot loader
def bootloader(bootstraps: list):
    keyword = 'bootloader'
    for b in bootstraps:
        assert isinstance(b, dict) and keyword in b, 'bootloader not found: {}'.format(b)
        args = dict(b)
        func = args.pop(keyword)
        result = getobj(func)(**args)
        print(result)
        

if __name__ == '__main__':
    os.system('pwd')
    assert len(sys.argv) == 2, 'please use like `python -m paiclient.booting <your/bootstraps>.json`'
    bootstraps = json.load(sys.argv[1])
    bootloader(bootstraps)