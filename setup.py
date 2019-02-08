import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(name='pymtattl',
      version='1.1.1',
      description='Download and store MTA turnstile data',
      long_description=read('README.md'),
      long_description_content_type="text/markdown",
      url='https://github.com/Ritaotao/pymtattl',
      author='Ritaotao',
      author_email='ritaotao28@gmail.com',
      license='MIT',    
      keywords=['mta', 'turnstile', 'data', 'traffic'],
      classifiers=[
        'Programming Language :: Python :: 3',
        'Topic :: Database',
        'Topic :: Utilities',
        'Topic :: Education',
      ],
      packages=['pymtattl'],
      install_requires=['beautifulsoup4', 'pandas', 'sqlalchemy'],
      )
