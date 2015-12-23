#!/usr/bin/env python
"""
Synchronize file systems with zfs send and receive

Gather information about zfs file systems locally and remotely in
order to facilitate synchronizing data across multiple systems.
"""

import os
import subprocess


class ZfsSend:
    """
    Interact with zfs send and receive
    """

    def __init__(self):
        self.hosts = []

    def volume_exists(self, volume, host=None):
        """
        Find if remote volume exists
        """
        try:
            fnull = open(os.devnull, 'w')
            command = ['zfs', 'list', volume]
            if host:
                command = ['ssh', host, 'sudo'] + command
            out = subprocess.check_call(
                command, stdout=fnull, stderr=subprocess.STDOUT)
            fnull.close()
            return True
        except Exception:
            fnull.close()
            return False

    def get_snapshots(self, volume, host=None):
        """
        Return all snapshots for a volume
        """
        try:
            command = ['zfs', 'list', '-r', '-t',
                       'snapshot', '-o', 'name', volume]
            if host:
                command = ['ssh', host, 'sudo'] + command
            out = subprocess.check_output(command)
            snapshot = []
            snapshots = out.split('\n')[1:-1]
            for i in snapshots:
                parts = i.split('@')
                snapshot.append(parts[1])
            return snapshot
        except Exception:
            raise

    def has_vol_snapshots(self, volume, host=None):
        """
        Check if a volume exists and has snapshots
        """
        try:
            if self.volume_exists(volume, host):
                snapshots = self.get_snapshots(volume, host)
            return snapshots
        except Exception:
            return False

    def snapshot_diff(self, volume, host):
        """
        Check if remote and local snapshots differ
        """
        try:
            remote = self.has_vol_snapshots(volume, host)
            if remote:
                local = self.has_vol_snapshots(volume)
                diff = list(set(local) - set(remote))
                if diff:
                    return diff
                else:
                    return False
            else:
                return False
        except Exception:
            raise


    def replicate(self, volume, host):
        """
        Replicate zfs volumes
        """
        try:
            snaps = None
            local = self.get_snapshots(volume)
            remote = self.has_vol_snapshots(volume, host)
            if remote:
                diff = list(set(local) - set(remote))
                if diff:
                    options = '-I'
                    snaps = [
                        '@{}'.format(remote[-1]),
                        '{}@{}'.format(volume, local[-1])
                        ]
            else:
                options = '-R'
                snaps = ['{}@{}'.format(volume, local[-1])]
            if snaps:
                send = subprocess.Popen(
                    ['zfs', 'send', options] + snaps, stdout=subprocess.PIPE)
                receive = subprocess.Popen(
                    ['ssh', host, 'sudo', 'zfs', 'receive', volume],
                    stdin=send.stdout, stdout=subprocess.PIPE)
                send.stdout.close()
                output = receive.communicate()
                return 'Success'
            else:
                return 'Everything is up to date'
        except Exception:
            raise

    def take_snapshot(self, volume):
        """
        Take a snapshot of a volume with timestamp
        """
