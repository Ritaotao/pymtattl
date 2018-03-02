from setuptools import setup

setup(name='pymtattl',
      packages=['pymtattl'],
      version='0.1',
      license='MIT',
      author='Ritaotao',
      author_email='ritaotao28@gmail.com',
      description='Download and store MTA turnstile data',
      url='http://github.com/Ritaotao/pymtattl',
      install_requires=['bs4', 'pandas'],
      keywords=['mta', 'data'],
      classifiers=[
        'Programming Language :: Python :: 3',
        'Topic :: Data',
        'Topic :: Utilities',
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: Science/Research',
        'Operating System :: Windows',
        'Topic :: Education'
        ]
      )
