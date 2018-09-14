import os
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

_, filename = os.path.split(sys.argv[1])
plt.suptitle(filename)

def to_seconds(s):
    return pd.to_datetime(np.ceil(s / 1000), unit='s')

for csvfile in sys.argv[1:]:
    d = pd.read_csv(csvfile)
    # d = np.array_split(d, 3)[1]

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
    print(d['d_accept'].quantile([.68, .95, .997]))
    print(d['d_commit'].quantile([.68, .95, .997]))

    tps_accept = tps_accept.reindex(range(math.ceil(max(d['rt_accept']))), fill_value=0)
    S_ACCEPT.append(tps_accept)
    M_ACCEPT.append({
        'median': round(tps_accept.median()),
        'mean': round(tps_accept.mean())
    })

    tps_commit = tps_commit.reindex(range(math.ceil(max(d['rt_commit']))), fill_value=0)
    S_COMMIT.append(tps_commit)
    M_COMMIT.append({
        'median': round(tps_commit.median()),
        'mean': round(tps_commit.mean())
    })


for i, tps in enumerate(S_ACCEPT):
    S_ACCEPT[i] = tps.reindex(range(longest), fill_value=0)

for i, tps in enumerate(S_COMMIT):
    S_COMMIT[i] = tps.reindex(range(longest), fill_value=0)

p[0][0].set_title('Time to accept transactions')
p[0][0].hist(H_ACCEPT, bins=25)
#p[0][0].set_yscale('log')
p[0][0].set_xlabel('Time (seconds)')
p[0][0].set_ylabel('Number of transactions')

p[1][0].set_title('Time to finalize transactions')
p[1][0].hist(H_COMMIT, bins=25)
#p[1][0].set_yscale('log')
p[1][0].set_xlabel('Time (seconds)')
p[1][0].set_ylabel('Number of transactions')

p[0][1].set_title('Accepted transactions per second')
#p[0][1].set_yscale('log')
p[0][1].set_xlabel('Time (seconds)')
p[0][1].set_ylabel('Amount')
for tps, vals in zip(S_ACCEPT, M_ACCEPT):
    x = tps.index
    p[0][1].scatter(x, tps, s=1)
    #p[0][1].plot(x, [median] * len(x), '--k')

    bbox_props = dict(boxstyle="round", fc="w", ec="0.5", alpha=0.9)
    p[0][1].text(0, 100,
            'median: {median} tx/s\nmean: {mean} tx/s'.format(**vals),
            ha="left", va="bottom", size=12,
            bbox=bbox_props)


p[1][1].set_title('Finalized transactions per second')
#p[1][1].set_yscale('log')
p[1][1].set_xlabel('Seconds')
p[1][1].set_ylabel('Amount')
for tps, vals in zip(S_COMMIT, M_COMMIT):
    x = tps.index
    p[1][1].scatter(x, tps, s=1)
    #p[1][1].plot(x, [median] * len(x), '--k')
    bbox_props = dict(boxstyle="round", fc="w", ec="0.5", alpha=0.9)
    p[1][1].text(0, 100,
            'median: {median} tx/s\nmean: {mean} tx/s'.format(**vals),
            ha="left", va="bottom", size=12,
            bbox=bbox_props)

plt.subplots_adjust(left=0.07, right=0.99, bottom=0.05, top=0.92, wspace=0.15, hspace=0.3)
plt.show()
