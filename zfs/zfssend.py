#!/usr/bin/env python
"""
Synchronize file systems with zfs send and receive

Gather information about zfs file systems locally and remotely in
order to facilitate synchronizing data across multiple systems.
"""

import os
import subprocess
import sys
import atexit


class ZfsSend:
    """
    Interact with zfs send and receive
    """

    def __init__(self):
        self.hosts = []

    def volume_exists(self, volume, host=None, sudo=True):
        """
        Find if remote volume exists
        """
        try:
            fnull = open(os.devnull, 'w')
            command = ['zfs', 'list', volume]
            if host:
                command = ['ssh', host, 'sudo'] + command
            elif sudo:
                command = ['sudo'] + command
            out = subprocess.check_call(
                command, stdout=fnull, stderr=subprocess.STDOUT)
            fnull.close()
            return True
        except Exception:
            fnull.close()
            return False

    def get_snapshots(self, volume, host=None, sudo=True):
        """
        Return all snapshots for a volume
        """
        try:
            command = ['zfs', 'list', '-r', '-t',
                       'snapshot', '-o', 'name', volume]
            if host:
                command = ['ssh', host, 'sudo'] + command
            elif sudo:
                command = ['sudo'] + command
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
        if self.volume_exists(volume, host):
            snapshots = self.get_snapshots(volume, host)
            return snapshots
        else:
            return False

    def snapshot_diff(self, volume, host):
        """
        Check if remote and local snapshots differ
        """
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

    def replication_type(self, volume, host):
        """
        Determine the type of replication to perform

        Detects whether target system has zfs volume
        If detected, determines whether incremental sync is possible
        """
        snaps, force = None, None
        local_snapshots = self.get_snapshots(volume)
        remote_snapshots = self.has_vol_snapshots(volume, host)
        snaps = ['{}@{}'.format(volume, local_snapshots[-1])]
        send_options = ['-R']
        recv_options = ['-F', '-d']
        if remote_snapshots:
            if remote_snapshots[-1] == local_snapshots[-1]:
                return False
            # Check if common snapshot exists
            elif remote_snapshots[-1] in local_snapshots:
                print 'Found incremental snapshot point for {} at {}'.format(volume, remote_snapshots[-1])
                send_options.append('-I')
                snaps.insert(0, '@{}'.format(remote_snapshots[-1]))
            elif remote_snapshots[-1] not in local_snapshots:
                print 'No common incremental snapshot found. Removing snapshots and forcing re-sync.'
                remove = subprocess.check_output(
                    ['ssh', host, 'sudo', '/usr/local/bin/zfs_snapshot',
                     '-k', '0', '-V', volume])
            else:
                print "I don't know how I got here"
                return False
        else:
            print '{} does not exist on the target. Starting replication.'.format(volume)
        return send_options, recv_options, snaps

    def replicate(self, volume, host, sudo=True, target_volume='backup-tank'):
        """
        Replicate zfs volumes
        """
        atexit.register(self.remove_lock, volume)
        repl_type = self.replication_type(volume, host)
        if repl_type:
            send_options, recv_options, snaps = repl_type
            send_command = ['zfs', 'send'] + send_options + snaps
            recv_command = ['ssh', host, 'sudo', 'zfs', 'receive'] + recv_options + [target_volume]
            if sudo:
                send_command = ['sudo'] + send_command
            self.lock_file(volume)
            send = subprocess.Popen(send_command, stdout=subprocess.PIPE)
            pv = subprocess.Popen(['pv'], stdin=send.stdout, stdout=subprocess.PIPE)
            receive = subprocess.Popen(recv_command, stdin=pv.stdout, stdout=subprocess.PIPE)
            send.stdout.close()
            output = receive.communicate()
            if output[0]:
                return output[0]
            elif 'cannot receive' in output[0]:
                return 'Replication failed with with message: {}'.format(output[0])
            else:
                return 'Replication of {} completed successfully with snapshot from {}'.format(volume, snaps[-1].split('@')[-1])
        else:
            return 'Volume {} is up to date'.format(volume)

    def take_snapshot(self, volume):
        """
        Take a snapshot of a volume with timestamp
        """

    @classmethod
    def lock_file(cls, volume):
        """
        Create a lock file for tracking state of replication
        """
        try:
            lockfile = '/{}/.replication_lock'.format(volume)
            if os.path.isfile(lockfile):
                print 'A lockfile exists'
                sys.exit(3)
            lock = open(lockfile, 'w')
            lock.close()
        except Exception:
            print 'Cannot create a lockfile at {}'.format(volume)
            sys.exit(4)

    @classmethod
    def remove_lock(cls, volume):
        """
        Remove the lockfile
        """
        try:
            lockfile = '/{}/.replication_lock'.format(volume)
            if os.path.isfile(lockfile):
                os.remove(lockfile)
        except Exception:
            print 'Cannot remove lockfile {}'.format(lockfile)
