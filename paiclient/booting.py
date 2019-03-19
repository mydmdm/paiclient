import os
import sys
import re
import importlib
import logging
import shutil
import random
import simplejson as json
from hdfs import InsecureClient
from functools import partial    
from argparse import ArgumentParser

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger('SysInfo')

# common
def getobj(name: str):
    mod_name, func_name = name.rsplit('.',1)
    mod = importlib.import_module(mod_name)
    return getattr(mod, func_name)


def hello_world():
    print('hello, world!')


def run_commands(commands: list):
    cmds = ' && '.join(commands)
    print(cmds)
    os.system(cmds)


def shuffle_text(text: str, seed=None, seed_env: str=None):
    "randomly shuffling a text string by given seed"
    if seed is None and seed_env is not None:
        seed = os.environ[seed_env]
    if seed is not None:
        random.seed(seed)
    s = list(text)
    random.shuffle(s)
    return ''.join(s)


def shuffle_back_text(text: str, seed=None, seed_env: str=None):
    "inverse function of shuffle_text"
    if seed is None and seed_env is not None:
        seed = os.environ[seed_env]
    assert seed is not None, 'cannot shuffle back without specifying a seed'
    random.seed(seed)
    idx = list(range(len(text)))
    random.shuffle(idx)
    idx_hook = {origin:shuffled for shuffled,origin in enumerate(idx)}
    if isinstance(text, str):
        s =[text[idx_hook[i]] for i in range(len(text))]
    else:
        s =[chr(text[idx_hook[i]]) for i in range(len(text))]
    return ''.join(s)

        
# commands for git
def git_config(user: str, email: str, keystr: str=None, seed_env: str='RANDOM_KEY'):
    run_commands([
        'git config --global user.name %s' % (user),
        'git config --global user.email %s' % (email),
    ])
    if keystr is not None:
        ssh_dir = os.path.expanduser('~/.ssh')
        os.makedirs(ssh_dir, exist_ok=True)
        key_file = os.path.join(ssh_dir, 'id_rsa')
        with open(key_file, 'w') as fn:
            fn.write(shuffle_back_text(keystr, seed_env=seed_env))
        run_commands([
            'chmod 400 {}'.format(key_file),
            'ssh -o StrictHostKeyChecking=no git@github.com',
        ])
    logger.info('git configured')

    
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
    logger.info('hdfs file downloaded %s -> %s' %(hdfs_path, local_path))
    return local_file
    
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
        logger.info('bootloader <- {}'.format(func))
        getobj(func)(**args)

        
if __name__ == '__main__':
    os.system('pwd')
    parser = ArgumentParser('commands to booting the job')
    parser.add_argument('--json', help='specify a local json file as bootstraps')
    parser.add_argument('--hdfs', help='specify a hdfs file as the bootstraps')
    args = parser.parse_args()
    if args.hdfs is not None:
        logger.info('downloading bootstraps ..')
        args.json = hdfs_download(args.hdfs, os.path.expanduser('~'), trans_args={'overwrite': True})
    with open(args.json, 'r') as fn:
        bootstraps = json.load(fn)
    bootloader(bootstraps)