import pydot as pt
import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os

model_dir = 'model7'
os.mkdir(model_dir)
epoch = 5
model_name = 'epoch_' + str(epoch) + '_conv(32,(7,7),relu)_batchNorm_maxPool((2,2),2)_(relu)_(1024)_(softmax).png'

def plot_history(history):
    hist = pd.DataFrame(history.history)
    hist['epoch'] = history.epoch
    plt.figure()
    plt.xlabel('Epoch')
    plt.ylabel('Learning Rate')
    plt.plot(hist['epoch'], hist['loss'],
           label='Loss')
    plt.plot(hist['epoch'], hist['acc'],
           label='Acc')
    plt.ylim([0,1])
    plt.legend()
    plt.savefig(model_dir+'/learning_curve_'+model_name)

(x_train, y_train) = (np.load('input/x_train.npy'), np.load('input/y_train.npy'))
(x_test, y_test) = (np.load('input/x_test.npy'), np.load('input/y_test.npy'))

inputs = tf.keras.layers.Input(shape=(28, 28, 3), name='inputs')

feature_map = tf.keras.layers.MaxPool2D(pool_size=(2, 2), strides=2)(
	tf.keras.layers.BatchNormalization(axis = 3)(
		tf.keras.layers.Conv2D(32, (7, 7), activation='relu')(
			inputs)))

outputs = tf.keras.layers.Dense(96, activation='softmax', name='output')(tf.keras.layers.Dense(1024, activation='relu')(tf.keras.layers.Flatten()(feature_map)))

model = tf.keras.Model(inputs=inputs, outputs=outputs)

model.compile(optimizer='adam',
  loss='sparse_categorical_crossentropy',
  metrics=['accuracy'])

print("#############################################				Training				##############################################")
model_history = model.fit(x_train, y_train, epochs=epoch)

plot_history(model_history)

print("#############################################				Testing					###############################################")
result = model.evaluate(x_test, y_test)

tf.keras.utils.plot_model(
    model,
    to_file=model_dir+'/model_'+model_name,
    show_shapes=False,
    show_layer_names=True,
    rankdir='TB'
)

model.save(model_dir + '/model')

print("###############################          END				###################################################")

f = open(model_dir + '/result', 'a')
f.write(str(result))
f.close()