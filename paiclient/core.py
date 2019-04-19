import os
import re
import uuid
import json
from paiclient.utils import update_obj, get_response
from paiclient.storage import Storage

def in_job_container(varname: str='PAI_CONTAINER_ID'):
    """
    to check whether it is inside a job container (by checking environmental variables)
        varname (str, optional): Defaults to 'PAI_CONTAINER_ID'. 
    """
    if not os.environ.get(varname, ''):
        return False
    return True


class Job:

    def __init__(self, version: str='1.0', **kwargs):
        self.config = dict()
        self.version = version
        if self.version == '1.0':
            for k in ['jobName', 'image', 'codeDir', 'dataDir', 'outputDir']:
                self.config[k] = ''
            self.config.update(taskRoles=[], jobEnvs={})
        self.config.update(kwargs)
        self.sources, self.code_dir, self.data_dir = [], None, None

    @property
    def minimal_resources(self):
        return dict(taskNumber=1, cpuNumber=4, gpuNumber=0, memoryMB=8192)

    def add_task_role(self, name: str, command: str, **kwargs):
        task_role = dict(name=name, command=command)
        update_obj(task_role, self.minimal_resources)
        update_obj(task_role, kwargs)
        self.config['taskRoles'].append(task_role)
        return self

    def add_source_codes(self, 
        files, # type: Union[list[str], str]
        code_dir: str=None
        ):
        """
        [summary]
        
        Args:
            files ([type]): [description]
        
        Returns:
            [type]: [description]
        """

        self.sources.extend(files if isinstance(files, list) else [files])
        self.code_dir = code_dir
        return self

    @staticmethod
    def simple(jobName: str, image: str, command: str, resources: dict={}, use_uuid: bool=True, **kwargs):
        """
        return a job config object from only necessary information
        
        Args:
            jobName (str): [description]
            image (str): [description]
            command (str): [description]
            resources (dict, optional): Defaults to {}. [description]
            use_uuid (bool, optional): Defaults to True. to add a uuid string after jobName to avoid name conflict
        
        Returns:
            [type]: [description]
        """
        if use_uuid:
            jobName += '_{}'.format(uuid.uuid4().hex) 
        return Job(
            jobName=jobName, image=image, **kwargs
        ).add_task_role(name='main', command=command, **resources)


class Client:

    def __init__(self, pai_uri: str, user: str=None, passwd: str=None, hdfs_web_uri: str=None):
        self.pai_uri = pai_uri
        self.user, self.passwd = user, passwd
        self.storages = []
        self.add_storage(hdfs_web_uri=hdfs_web_uri)
    
    @property
    def storage(self):
        return self.storages[0] if len(self.storages) >0 else None
             
    def add_storage(self, hdfs_web_uri: str=None):
        "initialize the connection information"
        if hdfs_web_uri:
            self.storages.append(Storage(protocol='hdfs', url=hdfs_web_uri, user=self.user))
        return self
    
    def get_token(self, expiration=3600):
        """
        [summary]
            expiration (int, optional): Defaults to 3600. [description]
        
        Returns:
            OpenPAIClient: self
        """

        self.token = get_response(
            '{}/rest-server/api/v1/token'.format(self.pai_uri), 
            body={
                'username': self.user, 'password': self.passwd, 'expiration': expiration
            }
        ).json()['token']
        return self

    def submit(self, job: Job, allow_job_in_job: bool=False):
        """
        [summary]
        
        Args:
            job (Job): job config
            allow_job_in_job (bool, optional): Defaults to False. [description]
        
        Returns:
            [str]: job name
        """

        if not allow_job_in_job:
            assert not in_job_container(), 'not allowed submiting jobs inside a job'
        if len(job.sources) > 0:
            assert job.code_dir, 'codeDir not specified'
            remote_root = '{}/{}/code'.format(job.code_dir, job.config['jobName'])
            job.config['codeDir'] = "$PAI_DEFAULT_FS_URI{}".format(remote_root)
            for file in job.sources:
                self.storage.upload(local_path=file, remote_path='{}/{}'.format(remote_root, file))
        get_response(
            '{}/rest-server/api/v1/user/{}/jobs'.format(self.pai_uri, self.user),
            headers = {
                'Authorization': 'Bearer {}'.format(self.token),
                'Content-Type': 'application/json'
            },
            body = job.config, 
            allowed_status=[202]
        )
        return '{}/job-detail.html?username={}&jobName={}'.format(self.pai_uri, self.user, job.config['jobName'])

    def jobs(self, jobName: str=None, name_only: bool=False):
        """
        query the list of jobs
            jobName (str, optional): Defaults to None. [description]
            name_only (bool, optional): Defaults to False. [description]
        
        Returns:
            [type]: [description]
        """

        pth = '{}/rest-server/api/v1/user/{}/jobs'.format(self.pai_uri, self.user)
        if jobName is not None:
            pth += '/' + jobName
        job_list = get_response(
            pth,
            headers = {}, method='GET'
        ).json()
        return [j['name'] for j in job_list] if name_only else job_list


    @staticmethod
    def from_envrons(**kwargs):
        return PAI(user=os.environ['PAI_USER_NAME'], ip=get_ip(os.environ['PAI_DEFAULT_FS_URI']), **kwargs)

    