import csv
import numpy as np
from sklearn import datasets
from sklearn.svm import SVR, NuSVR
from sklearn.neighbors import KNeighborsRegressor as KNNR

def get_RMSE(y, y_predict):
    return ((y-y_predict)**2).mean()

def run_SVR(filename,svr_model):
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        csv_input = list(reader)
        csv_input.pop(0)
        x_matrix = []
        y_matrix = []
        for i in range(len(csv_input)):
            row=[ float(x) for x in csv_input[i]]
            x_matrix.append(row[0:-1])
            y_matrix.append(row[-1])

    x = np.array(x_matrix)
    y = np.array(y_matrix)
    random_permutation_index = np.random.permutation(x.shape[0])
    x = x[random_permutation_index]
    y = y[random_permutation_index]

    train_x = x[:-100]
    train_y = y[:-100]
    test_x = x[-100:]
    test_y = y[-100:]
    print(train_x.shape, test_x.shape)
    model = svr_model.fit(train_x, train_y)
    y_predict = model.predict(train_x)
    print('trainig RMSE = {}'.format(get_RMSE(train_y,y_predict)))
    y_predict = model.predict(test_x)
    print('test RMSE = {}'.format(get_RMSE(test_y,y_predict)))
    print()

filenames = ['input_2007.csv', 'input_2007_w5.csv']

for filename in filenames:
    for model_type in ['SVR', 'NuSVR']:
        for error_penalty in [1,10,100,1000]:
            if model_type == 'SVR':
                svr_model = SVR(C=error_penalty)
            elif model_type == 'NuSVR':
                svr_model = NuSVR(C=error_penalty)
            print('{}, error_penalty = {}, {}'.format(model_type, error_penalty, filename)
            run_SVR(filename, svr_model)

for filename in filenames:
    for weight in ['uniform', 'distance']:
        for neighbors in [3,5,7]:
            knn = KNNR(neighbors, weights=weight)
            print('neighbors = {}, weight = {}'.format(neighbors, weight))
            run_SVR(filename, knn)



