#!/usr/bin/env python
"""
Provide basic ssh

This is designed to provide basic ssh interaction.
"""

import paramiko


class Ssh:
    """
    Simple ssh
    """

    def __init__(self):
        self.ssh = paramiko.SSHClient()

    def open_connection(self, host, user=None, verbose=False):
        """
        Open a ssh connection

        If a username is supplied connect with that username.
        """
        try:
            if user:
                self.ssh.connect(host, username=user)
            else:
                self.ssh.connect(host)
            if verbose:
                print 'connecting to {}'.format(host)
        except Exception:
            if verbose:
                print 'ssh connection to {} failed. The host may not be up'.format(host)
            raise

    def close_connection(self):
        """
        Close a ssh connection
        """
        try:
            self.ssh.close()
        except Exception:
            print 'Cannot close ssh session'
            raise

    def run_command(self, command, ret=True):
        """
        Run the specified command on a connected system
        """
        try:
            stdin, stdout, stderr = self.ssh.exec_command(
                command)
            if ret:
                return stdout.readlines()
        except Exception:
            print 'The command {} cannot be run'.format(command)
