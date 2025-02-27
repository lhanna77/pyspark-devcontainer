import pandas as pd
import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler
from collections import Counter

from .configuration import *

def get_corner_predictions(historic_data,prediction_data):

    # Load datasets
    #historic_data = pd.read_csv("historic_dataset.csv")
    #prediction_data = pd.read_csv("prediction_dataset.csv")

    feature_list = ['1HC', '1AC', '2HC', '2AC', '1HC_Res','1AC_Res','2HC_Res','2AC_Res', 'PastForFirst','PastForSecond','PastForTotal','ResultTotalFirst','ResultTotalSecond','ResultTotal','PastAgainstTotal','HistoryOverFirst','HistoryOverSecond','HistoryOverBoth','HistoryOverTotal','HistoryOverTwo','HistoryOverThree','HistoryAgainstOverTwo']

    # Features and target variables
    X_train = historic_data[feature_list]
    y_train_one = historic_data['WinOverOne']
    y_train_two = historic_data['WinOverTwo']
    y_train_first = historic_data['WinOverFirst']
    y_train_second = historic_data['WinOverSecond']
    y_train_both = historic_data['WinOverBoth']
    y_train_total = historic_data['WinOverTotal']

    X_pred = prediction_data[feature_list]

    # Check for class imbalance
    print("Class distribution for y_train_one:", Counter(y_train_one))
    print("Class distribution for y_train_two:", Counter(y_train_two))
    print("Class distribution for y_train_first:", Counter(y_train_first))
    print("Class distribution for y_train_second:", Counter(y_train_second))
    print("Class distribution for y_train_both:", Counter(y_train_both))
    print("Class distribution for y_train_total:", Counter(y_train_total))

    # Preprocessing: Scaling
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_pred_scaled = scaler.transform(X_pred)

    # Train models with class weight adjustment
    model_one = RandomForestClassifier(random_state=42, class_weight='balanced')
    model_two = RandomForestClassifier(random_state=42, class_weight='balanced')
    model_first = RandomForestClassifier(random_state=42, class_weight='balanced')
    model_second = RandomForestClassifier(random_state=42, class_weight='balanced')
    model_both = RandomForestClassifier(random_state=42, class_weight='balanced')
    model_total = RandomForestClassifier(random_state=42, class_weight='balanced')

    model_one.fit(X_train_scaled, y_train_one)
    model_two.fit(X_train_scaled, y_train_two)
    model_first.fit(X_train_scaled, y_train_first)
    model_second.fit(X_train_scaled, y_train_second)
    model_both.fit(X_train_scaled, y_train_both)
    model_total.fit(X_train_scaled, y_train_total)

    # Predict on the prediction dataset
    y_pred_one = model_one.predict(X_pred_scaled)
    y_pred_two = model_two.predict(X_pred_scaled)
    y_pred_first = model_first.predict(X_pred_scaled)
    y_pred_second = model_second.predict(X_pred_scaled)
    y_pred_both = model_both.predict(X_pred_scaled)
    y_pred_total = model_total.predict(X_pred_scaled)

    # Add predictions to the prediction dataset
    prediction_data['WinOverOne_Predicted'] = y_pred_one
    prediction_data['WinOverTwo_Predicted'] = y_pred_two
    prediction_data['WinOverFirst_Predicted'] = y_pred_first
    prediction_data['WinOverSecond_Predicted'] = y_pred_second
    prediction_data['WinOverBoth_Predicted'] = y_pred_both
    prediction_data['WinOverTotal_Predicted'] = y_pred_total

    # Save only the prediction dataset with results
    with pd.ExcelWriter(f"{football_corners_output_path}prediction_data - {datetime.date.today()}.xlsx") as writer:  
        prediction_data.to_excel(writer, sheet_name=f'prediction_data', index=False)

    print("Prediction dataset with results saved successfully!")