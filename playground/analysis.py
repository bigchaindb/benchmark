import sys
import math
from itertools import repeat

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


H_ACCEPT = []
H_COMMIT = []
S_ACCEPT = []
S_COMMIT = []
M_ACCEPT = []
M_COMMIT = []

ts_min = []
ts_max = []

f, p = plt.subplots(2, 2)
longest = 0

def to_seconds(s):
    return pd.to_datetime(np.ceil(s / 1000), unit='s')

for csvfile in sys.argv[1:]:
    d = pd.read_csv(csvfile)

    min_ts = min(d['ts_send'])

    d['rt_send'] = (d['ts_send'] - min_ts) / 1000
    d['rt_accept'] = (d['ts_accept'] - min_ts) / 1000
    d['rt_commit'] = (d['ts_commit'] - min_ts) / 1000

    d['d_accept'] = (d['ts_accept'] - d['ts_send']) / 1000
    d['d_commit'] = (d['ts_commit'] - d['ts_accept']) / 1000

    longest = math.ceil(max(longest, max(d['rt_commit'])))

    tps_accept = d['rt_accept'].groupby(lambda x: round(d['rt_accept'][x])).count()
    tps_commit = d['rt_commit'].groupby(lambda x: round(d['rt_commit'][x])).count()

    # tps_accept.index = pd.to_timedelta(tps_accept, 's')
    # tps_commit.index = pd.to_timedelta(tps_commit, 's')

    H_ACCEPT.append(d['d_accept'])
    H_COMMIT.append(d['d_commit'])

    S_ACCEPT.append(tps_accept)
    S_COMMIT.append(tps_commit)

    M_ACCEPT.append(tps_accept.median())
    M_COMMIT.append(tps_commit.median())


for i, tps in enumerate(S_ACCEPT):
    S_ACCEPT[i] = tps.reindex(range(longest), fill_value=0)

for i, tps in enumerate(S_COMMIT):
    S_COMMIT[i] = tps.reindex(range(longest), fill_value=0)


p[0][0].set_title('time to accept tx')
p[0][0].hist(H_ACCEPT, bins=25)
#p[0][0].set_yscale('log')
p[0][0].set_xlabel('time to accept (s)')
p[0][0].set_ylabel('number of txs (log)')

p[1][0].set_title('time to finalize tx')
p[1][0].hist(H_COMMIT, bins=25)
#p[1][0].set_yscale('log')
p[1][0].set_xlabel('time to finalize (s)')
p[1][0].set_ylabel('number of txs (log)')

p[0][1].set_title('accepted txs per second')
#p[0][1].set_yscale('log')
p[0][1].set_xlabel('time')
p[0][1].set_ylabel('tx/s')
for tps, median in zip(S_ACCEPT, M_ACCEPT):
    x = tps.index
    p[0][1].scatter(x, tps, s=1)
    p[0][1].plot(x, [median] * len(x), '--k')

p[1][1].set_title('finalized txs per second')
#p[1][1].set_yscale('log')
p[1][1].set_xlabel('time')
p[1][1].set_ylabel('tx/s')
for tps in S_COMMIT:
    x = tps.index
    p[1][1].scatter(x, tps, s=1)
    p[1][1].plot(x, [median] * len(x), '--k')

plt.subplots_adjust(left=0.125, right=0.9, bottom=0.1, top=0.9, wspace=0.4, hspace=0.6)
plt.savefig('/home/vrde/Desktop/test.svg')
plt.show()
