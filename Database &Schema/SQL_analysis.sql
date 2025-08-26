USE Care_Stat


-- Total revenue and number of payments --
SELECT 
    COUNT(*) AS total_payments,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_payment,
    MIN(payment_date) AS first_payment,
    MAX(payment_date) AS last_payment
FROM Payments;


-- Monthly revenue --
SELECT 
    YEAR(payment_date) AS year,
    MONTH(payment_date) AS month,
    SUM(amount) AS monthly_revenue,
    COUNT(*) AS payment_count
FROM Payments
GROUP BY YEAR(payment_date), MONTH(payment_date)
ORDER BY year, month


-- Payment methods (which method is most commonly used?) --

SELECT 
    method,
    COUNT(*) AS count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM Payments
GROUP BY method
ORDER BY count DESC;


-- Patients with chronic diseases --
SELECT 
    c.disease_name,
    COUNT(*) AS patient_count
FROM ChronicDiseases c
GROUP BY c.disease_name
ORDER BY patient_count DESC;

-- Patients with more than one chronic disease  --

SELECT 
    patient_id,
    COUNT(*) AS chronic_diseases_count
FROM PatientChronicDiseases
GROUP BY patient_id
HAVING COUNT(*) > 1
ORDER BY chronic_diseases_count DESC;


-- Doctors by number of visits --
SELECT TOP 10
    d.doctor_id,
    d.first_name + ' ' + d.last_name AS doctor_name,
    d.specialization,
    COUNT(v.visit_id) AS total_visits,
    d.rating_avg,
    d.salary
FROM Doctors d
JOIN Visits v ON d.doctor_id = v.doctor_id
GROUP BY d.doctor_id, d.first_name, d.last_name, d.specialization, d.rating_avg, d.salary
ORDER BY total_visits DESC;


-- For patients who have more than one visit --
SELECT TOP 10
    p.patient_id,
    p.first_name + ' ' + p.last_name AS patient_name,
    p.age,
    p.gender,
    COUNT(v.visit_id) AS total_visits
FROM Patients p
JOIN Visits v ON p.patient_id = v.patient_id
GROUP BY p.patient_id, p.first_name, p.last_name, p.age, p.gender
HAVING COUNT(v.visit_id) > 1
ORDER BY total_visits DESC;


-- Departments by number of doctors --

SELECT 
    dept.department_name,
    COUNT(dd.doctor_id) AS doctor_count
FROM Departments dept
JOIN DoctorDepartment dd ON dept.department_id = dd.department_id
GROUP BY dept.department_name
ORDER BY doctor_count DESC;


-- New patients per month --

SELECT 
    YEAR(admission_date) AS year,
    MONTH(admission_date) AS month,
    COUNT(*) AS new_patients
FROM Patients
GROUP BY YEAR(admission_date), MONTH(admission_date)
ORDER BY year, month;