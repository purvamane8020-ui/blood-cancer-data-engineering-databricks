# =====================================================
# SPRINT 3 – AI MODEL DEVELOPMENT
# Blood Cancer Risk Prediction
# =====================================================

import pandas as pd
import numpy as np
import re

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

# =====================================================
# STEP 1: Load Gold Table
# =====================================================

print("Loading gold_patient_profile table...")

gold_df = spark.table("default.gold_patient_profile").toPandas()

print("Loaded rows:", len(gold_df))
print("Columns:", list(gold_df.columns))

# =====================================================
# STEP 2: Data Cleaning & Feature Engineering
# =====================================================

# Remove null outcomes
gold_df = gold_df.dropna(subset=["outcome_status"])

# Convert target variable to binary
gold_df["risk_label"] = gold_df["outcome_status"].apply(
    lambda x: 1 if str(x).lower() == "critical" else 0
)

# Convert age to numeric
gold_df["age"] = pd.to_numeric(gold_df["age"], errors="coerce").fillna(0)

# One-hot encoding categorical features
gold_df = pd.get_dummies(
    gold_df,
    columns=["gender", "blood_group", "cancer_type", "stage"],
    drop_first=True
)

# =====================================================
# STEP 3: Model Training
# =====================================================

# Remove non-feature columns safely
columns_to_drop = ["patient_id", "outcome_status", "risk_label"]
existing_cols = [col for col in columns_to_drop if col in gold_df.columns]

X = gold_df.drop(existing_cols, axis=1)
y = gold_df["risk_label"]

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Train Random Forest Model
model = RandomForestClassifier(
    n_estimators=100,
    random_state=42
)

model.fit(X_train, y_train)

# Evaluate
predictions = model.predict(X_test)
accuracy = accuracy_score(y_test, predictions)

print("Model Accuracy:", accuracy)
print("\nClassification Report:\n")
print(classification_report(y_test, predictions))

# =====================================================
# STEP 4: Generate Predictions
# =====================================================

gold_df["predicted_risk"] = model.predict(X)
gold_df["risk_probability"] = model.predict_proba(X)[:, 1]

# Risk Level
gold_df["risk_level"] = gold_df["predicted_risk"].apply(
    lambda x: "High Risk" if x == 1 else "Low Risk"
)

# Risk Score (0–100)
gold_df["cancer_risk_score"] = (
    gold_df["risk_probability"] * 100
).round(2)

# Risk Category
gold_df["risk_category"] = gold_df["cancer_risk_score"].apply(
    lambda x: "Critical" if x >= 80
    else "High" if x >= 50
    else "Medium" if x >= 25
    else "Low"
)

# =====================================================
# STEP 5: Clean Column Names (Delta Safe)
# =====================================================

def clean_column(col):
    return re.sub('[^A-Za-z0-9_]+', '_', col)

gold_df.columns = [clean_column(c) for c in gold_df.columns]

print("Columns cleaned for Delta compatibility.")

# =====================================================
# STEP 6: Save to Delta Table
# =====================================================

spark_ai_df = spark.createDataFrame(gold_df)

spark_ai_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("default.gold_patient_risk_prediction")

print("✅ AI Risk Table Created Successfully!")
