"""
binlog setup script.

"""
from setuptools import setup, find_packages
import os

HERE = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(HERE, 'README.rst')).read()
NEWS = open(os.path.join(HERE, 'TODO.rst')).read()

VERSION = '0.0.1'

setup(name='binlog',
      version=VERSION,
      description="Store/Recover python objects sequencially.",
      long_description=README + '\n\n' + NEWS,
      classifiers=[
          'Programming Language :: Python :: 3.4',
      ],
      keywords='event log',
      author='Roberto Abdelkader Mart\xc3\xadnez P\xc3\xa9rez',
      author_email='robertomartinezp@gmail.com',
      url='https://github.com/nilp0inter/binlog',
      license='GPLv3',
      packages=find_packages(exclude=["tests", "docs"]),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'bsddb3==6.1.0',
          'acidfile==1.2.1'
      ])
