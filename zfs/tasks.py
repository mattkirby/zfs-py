#!/usr/bin/env python
"""
Provide tasks for celery
"""

import paramiko
import time

from celery import Celery


app = Celery('tasks', backend='redis://localhost', broker='amqp://guest@localhost//')

@app.task
def send_snapshot(volume, source, destination):
    """
    Replicate a zfs volume from one host to another
    """
    try:
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(source)
        chan = ssh.get_transport().open_session()
        chan.exec_command(
            '/home/zfssend/run_zfssend.py -V {} -H {}'.format(volume, destination)
            )
        while not chan.exit_status_ready():
            time.sleep(1)
            return 'Exit status {}'.format(chan.recv_exit_status())
    except Exception:
        raise
