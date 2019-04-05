import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score

(x_train, y_train) = (np.load('input/x_train.npy'), np.load('input/y_train.npy'))

# print(x_train)

model=linear_model.LinearRegression()

model.fit(x_train,y_train)


y_pred=model.predict(x_train)
print(model.coef_)
print("error =>"+str(mean_squared_error(y_train,y_pred)))
print(f"variance =>{r2_score(y_train,y_pred)}")


plt.scatter(x_train[:100,0], y_train[:100],  color='black')
plt.plot(x_train[:100,0], y_pred[:100], color='blue', linewidth=1)


plt.xticks(())
plt.yticks(())

plt.show()