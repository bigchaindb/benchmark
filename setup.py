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
        'logstats~=0.3.0',
        'requests~=2.19.1',
        'cachetools~=2.1.0',
        'websockets~=6.0.0',
        'aiohttp~=3.0.0',
    ],
    entry_points={
        'console_scripts': [
            'bigchaindb-benchmark=bigchaindb_benchmark.commands:main',
            'bigchaindb-blaster=bigchaindb_benchmark.async.__init__:main',
        ],
    },
)
