from setuptools import setup

setup(name='pymtattl',
      version='0.1',
      description='Download and store MTA turnstile data',
      url='http://github.com/',
      author='Ritaotao',
      author_email='ritaotao28@gmail.com',
      license='MIT',
      packages=['pymtattl'],
      install_requires=['bs4', 'pandas'],
      zip_safe=False)
