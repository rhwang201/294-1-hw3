input_file = open('subcats.txt', 'r')
output_file = open('subcats_regularized.txt', 'w')
output = ''

line = 'THIS IS THE BEGINNING'
tokens = []
for line in input_file:
	tokens = line.strip('()\n').split(',')[0:3]
	tokens[0] = tokens[0].strip(" ' ")
	tokens[1] = tokens[1].strip(" ' ").upper()
	tokens[2] = tokens[2].strip(" ' ").replace(' ', '_').replace('\\n', '_')
	tokens = tokens[0] + ', ' + tokens[1] + ', ' + tokens[2]
	output+=tokens+'\n'
output_file.write(output)

output_file.close()
input_file.close()