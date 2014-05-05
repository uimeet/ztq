
import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.txt')).read()
CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()

requires = [ 'redis>=2.4.9', 'transaction',]

if not os.sys.platform.startswith('win'):
    requires.append('hiredis')

setup(name='ztq_core',
      version = '1.2.5',
      author="edo,Tianzhen",
      author_email="uimeet@gmail.com",
      url="https://github.com/uimeet/ztq",
      description=u"Zopen Task Queue Core",
      long_description=README + '\n\n' +  CHANGES,
      packages=find_packages(),
      license = "MIT",
      platforms=["Any"],
      keywords='Everydo queue ztq_core async',
      classifiers = [
          'Development Status :: 5 - Beta',
          'Environment :: Web Environment',
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Operating System :: OS Independent',
          'Topic :: Internet :: WWW/HTTP',
          ],
	  install_requires = requires,
)

