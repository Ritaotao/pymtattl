import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(name='pymtattl',
      packages=['pymtattl'],
      version='0.1.3',
      license='MIT',
      author='Ritaotao',
      author_email='ritaotao28@gmail.com',
      description='Download and store MTA turnstile data',
      long_description=read('README.md'),
      url='https://github.com/Ritaotao/pymtattl',
      download_url='https://github.com/Ritaotao/pymtattl/archive/0.1.3.tar.gz',
      install_requires=['beautifulsoup4', 'pandas'],
      keywords=['mta', 'data', 'traffic'],
      classifiers=[
        'Programming Language :: Python :: 3',
        'Development Status :: 2 - Pre-Alpha',
        'Operating System :: Microsoft :: Windows',
        'Topic :: Database',
        'Topic :: Utilities',
        'Topic :: Education',
        ]
      )
