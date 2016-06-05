import numpy as np
from sklearn.svm import SVR

###############################################################################
# Generate sample data
X = np.sort(5 * np.random.rand(40, 1), axis=0)
y = np.sin(X).ravel()

###############################################################################
# Add noise to targets
y[::5] += 3 * (0.5 - np.random.rand(8))

###############################################################################
# Fit regression model
svr_rbf = SVR(kernel='rbf', C=1e3)
y_rbf = svr_rbf.fit(X, y).predict(X)

print(X.shape)
print(y.shape)
for i in range(X.shape[0]):
    print(y[i],y_rbf[i])
