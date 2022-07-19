from setuptools import setup

setup(
   author="Sietse Snel",
   author_email="s.t.snel@uu.nl",
   description=('Tool for generating synthetic iRODS iCAT database contents'),
   install_requires=[
       'psycopg2-binary>=2.7.7',
       'faker==13.14.0'
   ],
   name='icat_data_generate',
   packages=['icat_data_generate', 'icat_data_generate.iterators'],
   entry_points={
       'console_scripts': [
           'icat-data-gen = icat_data_generate.command:entry'
       ]
   },
   version='0.0.1',
)
