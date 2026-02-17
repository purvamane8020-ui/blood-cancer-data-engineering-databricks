#silver layer code 
CREATE OR REFRESH LIVE TABLE silver_patients
AS
SELECT
    patient_id,
    age,
    gender,
    blood_group,
    ingestion_timestamp,
    source_system,
    record_processed_timestamp
FROM LIVE.bronze_patients_1;

CREATE OR REFRESH LIVE TABLE silver_lab_reports
AS
SELECT
    report_id,
    patient_id,
    wbc_count,
    rbc_count,
    platelets,
    ingestion_timestamp,
    source_system,
    record_processed_timestamp
FROM LIVE.bronze_lab_reports_1;


CREATE OR REFRESH LIVE TABLE silver_symptoms
AS
SELECT
    symptom_id,
    patient_id,
    symptom,
    severity,
    ingestion_timestamp,
    source_system,
    record_processed_timestamp
FROM LIVE.bronze_symptoms_1;

CREATE OR REFRESH LIVE TABLE silver_treatment
AS
SELECT
    treatment_id,
    patient_id,
    treatment_type,
    ingestion_timestamp,
    source_system,
    record_processed_timestamp
FROM LIVE.bronze_treatment_1;

CREATE OR REFRESH LIVE TABLE silver_outcomes
AS
SELECT
    outcome_id,
    patient_id,
    status,
    ingestion_timestamp,
    source_system,
    record_processed_timestamp
FROM LIVE.bronze_outcomes_1;

CREATE OR REFRESH LIVE TABLE silver_cancer_stage
AS
SELECT
    stage_id,
    patient_id,
    cancer_type,
    stage,
    ingestion_timestamp,
    source_system,
    record_processed_timestamp
FROM LIVE.bronze_cancer_stage_1;

#gold layer code 
CREATE OR REFRESH LIVE TABLE gold_patient_profile
AS
SELECT
    p.patient_id,
    p.age,
    p.gender,
    p.blood_group,
    cs.cancer_type,
    cs.stage,
    o.status AS outcome_status
FROM LIVE.silver_patients p
LEFT JOIN LIVE.silver_cancer_stage cs USING (patient_id)
LEFT JOIN LIVE.silver_outcomes o USING (patient_id);

CREATE OR REFRESH LIVE TABLE gold_lab_summary
AS
SELECT
    patient_id,
    AVG(wbc_count) AS avg_wbc,
    AVG(rbc_count) AS avg_rbc,
    AVG(platelets) AS avg_platelets,
    COUNT(*) AS total_lab_reports
FROM LIVE.silver_lab_reports
GROUP BY patient_id;

CREATE OR REFRESH LIVE TABLE gold_symptom_summary
AS
SELECT
    patient_id,
    COUNT(*) AS symptom_count,
    MAX(severity) AS max_severity
FROM LIVE.silver_symptoms
GROUP BY patient_id;

CREATE OR REFRESH LIVE TABLE gold_treatment_summary
AS
SELECT
    patient_id,
    COUNT(*) AS total_treatments,
    COLLECT_SET(treatment_type) AS treatment_types
FROM LIVE.silver_treatment
GROUP BY patient_id;

CREATE OR REFRESH LIVE TABLE gold_master_table
AS
SELECT
    p.patient_id,
    p.age,
    p.gender,
    p.blood_group,
    p.cancer_type,
    p.stage,
    p.outcome_status,

    l.avg_wbc,
    l.avg_rbc,
    l.avg_platelets,
    l.total_lab_reports,

    s.symptom_count,
    s.max_severity,

    t.total_treatments,
    t.treatment_types

FROM LIVE.gold_patient_profile p
LEFT JOIN LIVE.gold_lab_summary l USING (patient_id)
LEFT JOIN LIVE.gold_symptom_summary s USING (patient_id)
LEFT JOIN LIVE.gold_treatment_summary t USING (patient_id);

