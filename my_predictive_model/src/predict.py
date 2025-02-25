import pandas as pd
import datetime
from .data_preprocessing import preprocess_data
from .model_training import train_model, predict
from .model_evaluation import evaluate_model

def get_corner_predictions(df_corners_history,df_corners_predict):

    # Define feature list
    # feature_list = ['1HC', '1AC', '2HC', '2AC', '1HC_Res', '1AC_Res', '2HC_Res', '2AC_Res', 'PastForFirst', 'PastForSecond', 'PastForTotal', 'ResultTotalFirst', 'ResultTotalSecond', 'ResultTotal', 'PastAgainstTotal', 'HistoryOverFirst', 'HistoryOverSecond', 'HistoryOverBoth', 'HistoryOverTotal', 'HistoryOverTwo', 'HistoryOverThree', 'HistoryAgainstOverTwo']
    feature_list = ['1HC', '1AC', '2HC', '2AC', 'PastForTotal', 'PastAgainstTotal', 'HistoryOverFirst', 'HistoryOverSecond', 'HistoryOverBoth', 'HistoryOverTotal', 'HistoryOverTwo', 'HistoryOverThree', 'HistoryAgainstOverTwo', 'HistoryOverTwoBoth']


    # Preprocess data
    X_train_scaled, y_train_one, y_train_first, y_train_second, y_train_both, y_train_total, y_train_two, X_pred_scaled = preprocess_data(df_corners_history, df_corners_predict, feature_list)

    # Train models
    model_first = train_model(X_train_scaled, y_train_first)
    model_second = train_model(X_train_scaled, y_train_second)
    model_both = train_model(X_train_scaled, y_train_both)
    model_total = train_model(X_train_scaled, y_train_total)
    model_one = train_model(X_train_scaled, y_train_one)
    model_two = train_model(X_train_scaled, y_train_two)

    # Predict
    y_pred_first = predict(model_first, X_pred_scaled)
    y_pred_second = predict(model_second, X_pred_scaled)
    y_pred_both = predict(model_both, X_pred_scaled)
    y_pred_total = predict(model_total, X_pred_scaled)
    y_pred_one = predict(model_one, X_pred_scaled)
    y_pred_two = predict(model_two, X_pred_scaled)

    # Add predictions to the prediction dataset
    df_corners_predict['WinOverFirst_Predicted'] = y_pred_first
    df_corners_predict['WinOverSecond_Predicted'] = y_pred_second
    df_corners_predict['WinOverBoth_Predicted'] = y_pred_both
    df_corners_predict['WinOverTotal_Predicted'] = y_pred_total
    df_corners_predict['WinOverOne_Predicted'] = y_pred_one
    df_corners_predict['WinOverTwo_Predicted'] = y_pred_two
    
    # Save only the prediction dataset with results
    with pd.ExcelWriter(f"/home/jovyan/code/files/output/football/corners/prediction_data - {datetime.date.today()}.xlsx") as writer:  
        df_corners_predict.to_excel(writer, sheet_name=f'prediction_data', index=False)

    print("Prediction dataset with results saved successfully!")