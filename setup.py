from setuptools import setup

setup(
    name='kottabooks',
    version='0.1.0',
    description='Jupyter integration for Kotta',
    long_description='Manage jobs on Kotta from Jupyter',
    url='https://github.com/yadudoc/TuringClient',
    author='Yadu Nand Babuji',
    author_email='yadu@uchicago.edu',
    license='Apache 2.0',
    package_data={'': ['LICENSE']},
    packages=['kotta', 'serialize'],
    install_requires=['ipython_genutils', 'requests'],
)
