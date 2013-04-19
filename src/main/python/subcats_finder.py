import os

#HIGH_LEVEL_CATEGORY needs to be in all capitals and underscores#
HIGH_LEVEL_CATEGORY = 'CHINA'
BFS_DEPTH_LIMIT = 5
#################################################################
input_file_name = 'subcats_regularized.txt'
output_file_name = os.getcwd() + '/categories/' + HIGH_LEVEL_CATEGORY
#################################################################
input_file = open(input_file_name, 'r')
document = input_file.read().split('\n')
output_file = open(output_file_name, 'w')

empty_set = set()
frontier = set()
frontier.add(HIGH_LEVEL_CATEGORY)
nextland = set()
#for i in range(4):
#	line = input_file.readline()
i = 0			#number of iteration
f, n = 1, 0		#sizes of frontier, nextland
k = 1			#size of output
final_output = HIGH_LEVEL_CATEGORY
#final_output = "\nvvvvvvvvv  Level: {0}; Size = {1}  vvvvvvvvvv\n".format(i, f) + HIGH_LEVEL_CATEGORY + '\n'
while (not frontier.issubset(empty_set)) and i < BFS_DEPTH_LIMIT:
	level_output = ''
	for line in document:
		tokens = line.strip('\n').split(', ')
		if len(tokens) < 3:
			break
		if tokens[1] in frontier:
			level_output = level_output + tokens[2] + ', '
			k += 1
			nextland.add(tokens[2])
			n += 1
	#print 'After querying categories on the {0}th level:\nfrontier size = {1}\n{2}\nnextland size = {3}\n{4}\n'.format(i, f, frontier, n, nextland)
	frontier, nextland = nextland.copy(), empty_set.copy()
	f, n = n, 0
	i+=1
	if f > 0:
		final_output = final_output + ', ' + level_output
		#final_output = final_output + "\nvvvvvvvvv  Level: {0}; Size = {1}  vvvvvvvvvv\n".format(i, f) + level_output + '\n'

#final_output = 'Total output size for query term \'{0}\' after {1} levels = {2}\n\n'.format(HIGH_LEVEL_CATEGORY, BFS_DEPTH_LIMIT, k) + final_output
print 'BFS on query term "{0}" stopped after {1} iterations...\nOutput size = {2}\n'.format(HIGH_LEVEL_CATEGORY, i, k)

output_file.write(final_output)

output_file.close()
input_file.close()
