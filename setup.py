from setuptools import setup, find_packages


setup(
    name='zfs',
    version='0.1.60',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'zfs_send = zfs.zfs_send:main',
            'zfs_snapshot = zfs.snapshot:main',
        ]
    },
    install_requires=['tendo'],
    author='Matt Kirby',
    author_email='kirby@puppetlabs.com',
    description='A library for interacting with zfs',
    license='Apache License 2.0',
    url='github.com:mattkirby/zfs-py'
)
