# used to print organize latency by topics
# requires arguments of filenames
# filenames are ordered first direct and then broker

import sys
import json
import matplotlib.pyplot as plt

hm = {}  #direct
hm2 = {} #broker

iters = sys.argv[1:]

#data for direct
for i in range(0, int(len(iters)/2)):

    filename = iters[i]

    f = open(filename)
    data = json.loads(f.read())

    for key in data:
        if key not in hm:
            hm[key] = []

        for dat in data[key]:
            hm[key].append(dat)

direct_data = []
direct_label = [key for key in hm]
for key in hm:
    direct_data.append(hm[key])


#data for broker
for i in range( int(len(iters)/2), len(iters)-1):
    filename = iters[i]

    f = open(filename)
    data = json.loads(f.read())

    for key in data:
        if key not in hm2:
            hm2[key] = []

        for dat in data[key]:
            hm2[key].append(dat)

broker_data = []
broker_label = [key for key in hm2]
for key in hm2:
    broker_data.append(hm2[key])


fig, axs = plt.subplots(1, 2)
axs[0].boxplot(direct_data)
axs[0].set_title("Latency by Topic - Direct (in ms)")
axs[0].set_xlabel(direct_label)
axs[1].boxplot(broker_data)
axs[1].set_title("Latency by Topic - Broker (in ms)")
axs[1].set_xlabel(broker_label)
plt.show()
