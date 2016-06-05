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
    model = svr_model.fit(train_x, train_y)
    y_train_result = model.predict(train_x)
    print('trainig RMSE = {}'.format(get_RMSE(train_y,y_train_result)))
    y_predict_result = model.predict(test_x)
    print('test RMSE = {}'.format(get_RMSE(test_y,y_predict_result)))
    print()
    return test_y, y_predict_result

svr_model = SVR(C=1)
with open('prediction1.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    y_truth, y_predict = run_SVR('input_2007_w5.csv', svr_model)
    for i in range(len(y_truth)):
        writer.writerow([y_truth[i] ,y_predict[i]])

svr_model = NuSVR(C=100)
with open('prediction2.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    y_truth, y_predict = run_SVR('input_2007.csv', svr_model)
    for i in range(len(y_truth)):
        writer.writerow([y_truth[i] ,y_predict[i]])





