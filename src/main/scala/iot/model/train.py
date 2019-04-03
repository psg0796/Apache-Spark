import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os

model_dir = 'model7'
os.mkdir(model_dir)
epoch = 5
model_name = 'epoch_' + str(epoch) + '_conv(32,(7,7),relu)_batchNorm_maxPool((2,2),2)_(relu)_(1024)_(softmax).png'

(x_train, y_train) = (np.load('input/x_train.npy'), np.load('input/y_train.npy'))

def plot_history(history):
	hist = pd.DataFrame(history.history)
	hist['epoch'] = history.epoch
	for x in hist:
		print(x)
	# plt.figure()
	# plt.xlabel('Epoch')
	# plt.ylabel('Loss')
	# plt.plot(hist['epoch'], hist['output_length_loss'],
	#        label='Length Loss')
	# plt.plot(hist['epoch'], hist['output_width_loss'],
	#        label='Width Loss')
	# plt.plot(hist['epoch'], hist['output_color_loss'],
	#        label='Color Loss')
	# plt.plot(hist['epoch'], hist['output_angle_loss'],
	#        label='Angle Loss')
	# plt.plot(hist['epoch'], hist['loss'],
	#        label='Train Loss')
	# plt.ylim([0,1])
	# plt.legend()
	# plt.savefig(model_dir+'/loss_plot_'+model_name)

inputs = tf.keras.layers.Input(shape=(28, 28, 3), name='inputs')

outputs = tf.keras.layers.Dense(1, activation='softmax', name='output')(tf.keras.layers.Dense(512, activation='relu')(tf.keras.layers.Dense(1024, activation='relu')(inputs)))

model = tf.keras.Model(inputs=inputs, outputs=outputs)

model.compile(optimizer='adam',
  loss='sparse_categorical_crossentropy',
  metrics=['accuracy'])

print("#############################################				Training				##############################################")
model_history = model.fit(x_train, y_train, epochs=epoch)

plot_history(model_history)

model.save(model_dir + '/model')

print("###############################          END				###################################################")