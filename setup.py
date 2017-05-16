from setuptools import setup, find_packages
import os

HERE = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(HERE, 'README.rst')).read()
CHANGELOG = open(os.path.join(HERE, 'CHANGELOG.rst')).read()

VERSION = '3.0.1'

setup(name='binlog',
      version=VERSION,
      description="Store/Recover python objects sequencially.",
      long_description=README + '\n\n' + CHANGELOG,
      classifiers=[
          'Programming Language :: Python :: 3.4',
          'Development Status :: 4 - Beta',
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
      ],
      extras_require={
          'migration':  ['bsddb3==6.1.0',
                         'acidfile==1.2.1'],
      })
