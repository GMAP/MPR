# Auxiliar functions
def start_plot(xlabel = None, ylabel = None):
    plt.clf()
    plt.cla()
    fig1, ax1 = plt.subplots(2,1)

    ax1[0].grid(b=True, which='major', color='k', alpha=0.3, linestyle=':', linewidth=1)
    ax1[1].grid(b=True, which='major', color='k', alpha=0.3, linestyle=':', linewidth=1)
    ax1[1].set_ylabel(ylabel)
    ax1[1].set_xlabel(xlabel)
    
    return fig1, ax1

def finish_and_save_plot(fig, plt, name):
    fig.set_size_inches(12, 4)
    save_plot(fig, plt, name)

def save_plot(fig, plt, name):
    for fmt in ['png']:
        plt.savefig("{}.{}".format(name,fmt), dpi=600, bbox_inches='tight')
    plt.close()

def every_nth(nums, nth):
    return nums[0::nth]


import json
import datetime
import time
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import ticker

y = []
y2 = []
x = []
dataID = 0

fileIn = open("stats_stage2.json", "r") 
with fileIn:
    j = json.load(fileIn)
    fileIn.close()

    for stage in j:
        for timestamp in j[stage]:
            counter = 0
            throughput = 0
            for compute in j[stage][timestamp]:
                counter += 1
                throughput += j[stage][timestamp][compute]["averageItemsConsumed"]
            x.append(dataID)
            dataID += 1
            y.append(counter)
            y2.append(throughput)

fig, ax1 = start_plot()
ax1[1].set_ylabel("# of Processes", fontsize=12)
ax1[1].set_xlabel("Timestamp", fontsize=12)

ax1[0].set_ylabel("Throughput [items/sec]", fontsize=12)

ax1[0].plot(x, y2, color='green')
ax1[0].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x,pos: format(x/1000,'g')+'K'))
ax1[1].step(x, y)

plt.suptitle('Adaptability in MPR', fontsize=16)
finish_and_save_plot(fig, plt, "adaptability_mpr")