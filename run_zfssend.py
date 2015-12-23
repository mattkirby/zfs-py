#!/usr/bin/env python
"""
Replicate ZFS volumes

A tool to replicate volumes
"""

#import daemon
import zfssend
import argparse


parse = argparse.ArgumentParser(description='A program to replicate ZFS volumes')
zfs = zfssend.ZfsSend()

parse.add_argument('-V', '--volume', help='The volume to replicate', required=True)
parse.add_argument('-H', '--host', help='The host to receive replicas', required=True)

args = parse.parse_args()

def replicate_volume(volume=args.volume, host=args.host):
    """
    Replicate a zfs volume
    """
    try:
        replicate = zfs.replicate(volume, host)
        print replicate
    except Exception:
        print 'There are no snapshots. No replication will occur for {}'.format(volume)

if __name__ == '__main__':
    replicate_volume()
