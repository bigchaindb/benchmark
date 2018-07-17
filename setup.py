from setuptools import setup, find_packages

setup(
    name='bigchaindb_benchmark',
    version='0.0.1',
    description='Command Line Interface to push transactions to BigchainDB',
    author='BigchainDB devs',
    packages=find_packages(),
    install_requires=[
        'bigchaindb-driver~=0.5.0',
        'coloredlogs~=7.3.0',
        'websocket-client',
        'logstats',
    ],
    entry_points={
        'console_scripts': [
            'bigchaindb-benchmark=bigchaindb_benchmark.commands:main',
        ],
    },
)
