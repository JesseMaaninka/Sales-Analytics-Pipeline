import redis
import json
import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder
import numpy as np

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

print("=== ML Model: Linear Regression on Sales Revenue ===\n")

# Load processed data from Redis
raw = r.get("sales_processed")
if not raw:
    print("No data found. Run spark_etl.py first.")
    exit()

records = json.loads(raw)
df = pd.DataFrame(records)

# Encode categorical columns
le_salesman = LabelEncoder()
le_product  = LabelEncoder()
le_city     = LabelEncoder()

df["SalesMan_enc"] = le_salesman.fit_transform(df["SalesMan"])
df["Product_enc"]  = le_product.fit_transform(df["Product"])
df["City_enc"]     = le_city.fit_transform(df["city"])

# Features and target
features = ["SalesMan_enc", "Product_enc", "UnitPrice", "quantity", "Age", "City_enc"]
X = df[features]
y = df["revenue"]

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

mlflow.set_experiment("Sales_Revenue_Prediction")

with mlflow.start_run():
    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mse    = mean_squared_error(y_test, y_pred)
    rmse   = np.sqrt(mse)
    r2     = r2_score(y_test, y_pred)

    # Log parameters and metrics to MLflow
    mlflow.log_param("model_type",  "LinearRegression")
    mlflow.log_param("test_size",   0.2)
    mlflow.log_param("random_state", 42)
    mlflow.log_metric("rmse", round(rmse, 4))
    mlflow.log_metric("r2_score", round(r2, 4))
    mlflow.log_metric("mse", round(mse, 4))

    # Log the model
    mlflow.sklearn.log_model(model, "linear_regression_model")

    print(f"  RMSE:     {rmse:.2f}")
    print(f"  R² Score: {r2:.4f}")
    print(f"  MSE:      {mse:.2f}\n")

    # Save predictions back to Redis for dashboard
    df["predicted_revenue"] = model.predict(X)
    r.set("sales_predictions", df[["order_id", "SalesMan", "Product",
                                    "revenue", "predicted_revenue"]].to_json(orient="records"))

print("  Model tracked in MLflow. Run: mlflow ui")
print("  Then open http://localhost:5000 to see the run.\n")
print("ML done. Run dashboard.py next.")