import sys
import math

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

SUBPLOTS = 4

bag = []
data = []
ts_min = []
ts_max = []

for _ in range(SUBPLOTS):
    bag.append([])

f, p = plt.subplots(SUBPLOTS)
longest = 0

def to_seconds(s):
    return pd.to_datetime(np.ceil(s / 1000), unit='s')

for csvfile in sys.argv[1:]:
    d = pd.read_csv(csvfile)
    data.append(d)

    min_ts = min(d['ts_send'])

    d['rt_send'] = (d['ts_send'] - min_ts) / 1000
    d['rt_accept'] = (d['ts_accept'] - min_ts) / 1000
    d['rt_commit'] = (d['ts_commit'] - min_ts) / 1000

    d['d_accept'] = (d['ts_accept'] - d['ts_send']) / 1000
    d['d_commit'] = (d['ts_commit'] - d['ts_accept']) / 1000

    longest = math.ceil(max(longest, max(d['rt_commit'])))

    tps_accept = d['rt_accept'].groupby(lambda x: round(d['rt_accept'][x])).count()
    tps_commit = d['rt_commit'].groupby(lambda x: round(d['rt_commit'][x])).count()

    bag[0].append(d['d_accept'])
    bag[1].append(d['d_commit'])
    bag[2].append(tps_accept)
    bag[3].append(tps_commit)

for i, tps in enumerate(bag[2]):
    bag[2][i] = tps.reindex(range(longest), fill_value=0)

for i, tps in enumerate(bag[3]):
    bag[3][i] = tps.reindex(range(longest), fill_value=0)

for d in data:
    print(d['d_accept'].quantile([.25, .50, .75, .95, .99]))
    print(d['d_commit'].quantile([.25, .50, .75, .95, .99]))
    print('Average:', len(d['rt_commit']) / max(d['rt_commit']))

p[0].set_title('Time To Accept Transaction')
p[0].hist(bag[0], bins=25)
p[0].set_yscale('log')
p[0].set_xlabel('seconds to accept')
p[0].set_ylabel('number of transactions (log)')

p[1].set_title('Time To Commit Transaction')
p[1].hist(bag[1], bins=25)
p[1].set_yscale('log')
p[1].set_xlabel('seconds to commit')
p[1].set_ylabel('number of transactions (log)')

p[2].set_title('Accepted Transactions Per Second')
for tps in bag[2]:
    p[2].plot(tps)

p[3].set_title('Committed Transactions Per Second')
for tps in bag[3]:
    p[3].plot(tps)

plt.show()
