import os
import re

from paiclient.utils import update_obj, get_response


class OpenPAIJobConfig:

    def __init__(self, **kwargs):
        for k in ['jobName', 'image', 'codeDir', 'dataDir', 'outputDir']:
            setattr(self, k, '')
        self.taskRoles, self.jobEnvs = [], {}
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def minimal_resources(self):
        return dict(taskNumber=1, cpuNumber=4, gpuNumber=0, memoryMB=8192)

    def add_task_role(self, name: str, command: str, **kwargs):
        task_role = dict(name=name, command=command)
        update_obj(task_role, self.minimal_resources)
        update_obj(task_role, kwargs)
        self.taskRoles.append(task_role)
        return self

    @staticmethod
    def simple(jobName, image, command, resources: dict={}, **kwargs):
        return OpenPAIJobConfig(
            jobName=jobName, image=image, **kwargs
        ).add_task_role(name='main', command=command, **resources)


class OpenPAIClient:

    def __init__(self, pai_uri: str, user: str=None, passwd: str=None, hdfs_web_uri: str=None):
        self.pai_uri = pai_uri
        self.user, self.passwd = user, passwd
        self.storages = []
        if hdfs_web_uri is not None:
            self.add_hdfs(hdfs_web_uri)
    
    @property
    def storage(self):
        return self.storages[0] if len(self.storages) >0 else None
             
    def add_hdfs(self, hdfs_web_uri: str):
        "initialize the connection information"
        from hdfs import InsecureClient
        self.storages.append(
            InsecureClient(hdfs_web_uri, user=self.user)
        )
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

    def submit(self, jobconf):
        job = jobconf if isinstance(jobconf, dict) else vars(jobconf)
        get_response(
            '{}/rest-server/api/v1/user/{}/jobs'.format(self.pai_uri, self.user),
            headers = {
                'Authorization': 'Bearer {}'.format(self.token),
                'Content-Type': 'application/json'
            },
            body = job, 
            allowed_status=[202]
        )
        return job['jobName']

    def jobs(self, jobName: str=None, name_only: bool=False):
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

    