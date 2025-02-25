import pandas as pd
from sklearn.preprocessing import StandardScaler

def preprocess_data(df_history, df_predict, feature_list):
    X_train = df_history[feature_list]
    y_train_one = df_history['WinOverOne']
    y_train_first = df_history['WinOverFirst']
    y_train_second = df_history['WinOverSecond']
    y_train_both = df_history['WinOverBoth']
    y_train_total = df_history['WinOverTotal']
    y_train_two = df_history['WinOverTwo']

    X_pred = df_predict[feature_list]

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_pred_scaled = scaler.transform(X_pred)

    return X_train_scaled, y_train_one, y_train_first, y_train_second, y_train_both, y_train_total, y_train_two, X_pred_scaled