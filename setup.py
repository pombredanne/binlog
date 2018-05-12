from setuptools import setup, find_packages
import os

HERE = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(HERE, 'README.rst')).read()
CHANGELOG = open(os.path.join(HERE, 'CHANGELOG.rst')).read()

VERSION = '5.1.0'

setup(name='binlog',
      version=VERSION,
      description="Store/Recover python objects sequencially.",
      long_description=README + '\n\n' + CHANGELOG,
      classifiers=[
          'Programming Language :: Python :: 3',
          'Development Status :: 5 - Production/Stable',
          'Topic :: Database',
          'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)'
      ],
      keywords='binary event log',
      author='Roberto Abdelkader Martínez Pérez',
      author_email='robertomartinezp@gmail.com',
      url='https://github.com/nilp0inter/binlog',
      license='LGPLv3',
      packages=find_packages(exclude=["tests", "docs"]),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'lmdb==0.92'
      ])
