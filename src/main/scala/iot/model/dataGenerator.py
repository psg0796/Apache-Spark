import numpy as np
# from kafka import KafkaConsumer
# consumer = KafkaConsumer('out1', group_id='iot', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest', enable_auto_commit=False)
x_train = []
y_train = []
i = 0
print("Starting..................................................................................")
# for message in consumer:
# 	print(i)
# 	i = i + 1
# 	msg = message.value.decode("utf-8")
# 	filteredMsg = ''
# 	for x in range(len(msg)):
# 		if(msg[x] == '(' and x>0):
# 			filteredMsg = msg[x+1:len(msg)-2].split(',')
# 	result = []
# 	for x in filteredMsg:
# 		result.append(float(x))
# 	if(len(result) == 6):
# 		x_train.append(result[1:])
# 		y_train.append(result[0])

i = 0
with open("result.txt", "r") as consumerRecords:
	for msg in consumerRecords:
		for x in range(len(msg)):
			if(msg[x] == '(' and x>0):
				filteredMsg = msg[x+1:len(msg)-3].split(',')
		result = []
		for x in filteredMsg:
			result.append(float(x))
		if(len(result) == 6):
			x_train.append(result[1:])
			y_train.append(result[0])
		i = i + 1
		print(i)

np.save('input/x_train',np.asarray(x_train))
np.save('input/y_train',np.asarray(y_train))
