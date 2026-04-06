from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split

def train_model(df, target):
    features = df[[
        "PTS_LAST_5",
        "AST_LAST_5",
        "REB_LAST_5"
    ]]

    y = df[target]

    X_train, X_test, y_train, y_test = train_test_split(
        features, y, test_size=0.2
    )

    model = GradientBoostingRegressor()
    model.fit(X_train, y_train)

    return model
