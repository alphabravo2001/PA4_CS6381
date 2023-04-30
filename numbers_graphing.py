# used to organize latency by num of pub/subs
# requires arguments of filenames
# filenames are ordered first direct and then broker
# the 4 trials were 1 pub/sub, 3 pub/sub, 7 pub/sub, 11 pub/sub
# pub and sub number are same for each trial

import sys
import json
import matplotlib.pyplot as plt

hm = {}   #direct
hm2 = {}  #broker

x_axis = [1,3,7,11]

iters = sys.argv[1:]
x_idx = 0

#data for direct
for i in range(0, int(len(iters)/2)):

    key = str(x_axis[x_idx])
    hm[key]= []

    filename = iters[i]

    f = open(filename)
    data = json.loads(f.read())

    for k in data:
        for dat in data[k]:
            hm[key].append(dat)

    x_idx += 1


x_idx = 0


#data for broker
for i in range( int(len(iters)/2), len(iters)):

    key = str(x_axis[x_idx])
    hm2[key] = []

    filename = iters[i]

    f = open(filename)
    data = json.loads(f.read())

    for k in data:
        for dat in data[k]:
            hm2[key].append(dat)

    x_idx += 1


x_idx = 0



direct_data = []
for key in hm:
    direct_data.append(hm[key])

broker_data = []
for key in hm2:
    print (key)
    broker_data.append(hm2[key])



fig, axs = plt.subplots(1, 2)
axs[0].boxplot(direct_data)
axs[0].set_title("Latency by Num of Pubs/Subs - Direct")
axs[0].set_xlabel(x_axis)
axs[1].boxplot(broker_data)
axs[1].set_title("Latency by Num of Pubs/Subs  - Broker")
axs[1].set_xlabel(x_axis)
plt.show()