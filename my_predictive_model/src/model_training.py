from sklearn.ensemble import RandomForestClassifier

def train_model(X_train, y_train):
    model = RandomForestClassifier(random_state=42, class_weight='balanced')
    model.fit(X_train, y_train)
    return model

def predict(model, X_pred):
    return model.predict(X_pred)