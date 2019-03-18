from setuptools import setup

setup(name='paiclient',
      version='0.1',
      description='A simple SDK for OpenPAI',
      url='https://github.com/mydmdm/paiclient',
      author='Yuqing Yang',
      author_email='justin.yqyang@gmail.com',
      license='MIT',
      packages=['paiclient'],
      install_requires=[
          'simplejson',
      ],      
      zip_safe=False)