#!/usr/bin/python2
import sched, time
from datetime import datetime
import psutil
import sys
import threading

procs = sys.argv[1:]

done = threading.Event()
def step(now):
    ts = datetime.utcnow().isoformat() + 'Z'

    ps = filter(lambda p: p.name() in procs, psutil.process_iter())
    for p in ps:
        name = p.name()
        pid = p.pid
        ppid = p.ppid()
        cpu = p.cpu_percent()
        rss = p.memory_info().rss
        times = p.cpu_times()

        row = [ts, pid, name, ppid, cpu, rss, times.user, times.system]

        print ",".join([str(c) for c in row])


    if len(ps) == 0:
        done.set()

# make sure to always sleep an absolute time
s = sched.scheduler(time.time, time.sleep)
wakeup = time.time()
while not done.is_set():
    wakeup += 1
    s.enterabs(wakeup, 1, step, (wakeup,))
    s.run()
