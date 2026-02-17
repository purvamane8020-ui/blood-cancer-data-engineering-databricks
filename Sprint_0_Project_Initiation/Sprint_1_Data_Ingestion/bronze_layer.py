CREATE OR REFRESH LIVE TABLE bronze_patients_1
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'bronze' AS source_system,
    current_timestamp() AS record_processed_timestamp
FROM cancer.cancer_schema.patients_uncleaned;

CREATE OR REFRESH LIVE TABLE bronze_lab_reports_1
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'bronze' AS source_system,
    current_timestamp() AS record_processed_timestamp
FROM cancer.cancer_schema.lab_reports_uncleaned;

CREATE OR REFRESH LIVE TABLE bronze_symptoms_1
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'bronze' AS source_system,
    current_timestamp() AS record_processed_timestamp
FROM cancer.cancer_schema.symptoms_uncleaned;

CREATE OR REFRESH LIVE TABLE bronze_treatment_1
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'bronze' AS source_system,
    current_timestamp() AS record_processed_timestamp
FROM cancer.cancer_schema.treatment_uncleaned;

CREATE OR REFRESH LIVE TABLE bronze_outcomes_1
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'bronze' AS source_system,
    current_timestamp() AS record_processed_timestamp
FROM cancer.cancer_schema.outcomes_uncleaned;

CREATE OR REFRESH LIVE TABLE bronze_cancer_stage_1
AS
SELECT
    *,
    current_timestamp() AS ingestion_timestamp,
    'bronze' AS source_system,
    current_timestamp() AS record_processed_timestamp
FROM cancer.cancer_schema.cancer_stage_uncleaned;
