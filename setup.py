from setuptools import setup

setup(name='fastcounting',
      version='0.2',
      description='Analysis tool for accounting data with redis and python.',
      url='github.com/elcolumbio/fastcounting',
      author='Florian Benkö',
      author_email='f.benkoe@innotrade24.de',
      license='Apache License, Version 2.0 (the "License")',
      package_data={'fastcounting.lua': ['*.lua']},
      packages=['fastcounting', 'fastcounting.lua'])
