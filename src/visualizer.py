import numpy as np
import os
from matplotlib import pyplot as plt

# load spreader data
def load_data(path, spreaders):
	error = 0
	with open(path, 'r') as fp:
		for line in fp:
			entry = line.split(',')
			if len(entry) < 2:
				error += 1
				continue
			if entry[0] not in spreaders:
				spreaders[entry[0]] = 0
			try:
				spreaders[entry[0]] += int(entry[1])
			except:
				error += 1
				continue
	print(error)
	return spreaders

# get the counts based on k
def get_count(spreaders):
	ks = np.arange(25, 35, 1)
	counts = []
	histo = []
	for k in ks:
		count = 0
		for spreader in spreaders:
			if spreaders[spreader] >= k:
				count += 1
		counts.append(count)
	for spreader in spreaders:
		if spreaders[spreader] > 50:
			histo.append(spreaders[spreader])
	return ks, counts, histo

# get the counts of data with respect to time
def get_count_by_time(k=100):
	times = np.arange(1, 11, 1)
	counts = []
	for t in times:
		spreaders = load_data('tmp/spreaders-' + str(t) +'.txt', {})
		count = 0
		for spread in spreaders:
			if spreaders[spread] >= k:
				count += 1
		counts.append(count)
	return times, counts

# plot the data
def plotter(x, y, xlable, title, histo=None):
	plt.title(title)
	plt.plot(x, y, '-o')
	plt.xlabel(xlable)
	plt.ylabel('Number of Superspreaders')
	plt.show()
	if histo != None:
		plt.hist(histo, bins=50)
		plt.title('Histogram for number of distinct host connections')
		plt.xlabel('Number of distinct host connections')
		plt.ylabel('Frequencies')
		plt.show()


# combine files
def combine_files():
	filenames = ['data_after_split/500000_1.csv']
	counter = 2
	while counter < 11:
		filenames.append('data_after_split/500000_' + str(counter - 1) + '.csv')
		with open('data_tmp/spreaders-' + str(counter - 1) + '.txt', 'w') as outfile:
			for fname in filenames:
				with open(fname) as infile:
					for line in infile:
						outfile.write(line)
		counter += 1

# calculate the average number of distinct hosts in each second
def get_average():
	avg = []
	spreaders = load_data('tmp/spreaders-5.txt', {})
	total = 0
	for spread in spreaders:
		total += spreaders[spread]
	avg.append(total / len(spreaders))
	avg = np.asarray(avg)
	print('Average ', avg.mean())

if __name__ == '__main__':
	# combine_files()
	# spreaders = {}
	# spreaders = load_data('tmp/spreaders-9.txt', spreaders)
	# ks, counts, histo = get_count(spreaders)
	# plotter(ks, counts, 'threshold k', 'Number of Superspreaders vs the threshold k', histo)
	t, counts = get_count_by_time()
	plotter(t, counts, 'time in seconds', 'Number of 100-Superspreaders vs time interval')
	# get_average()
	print('done')