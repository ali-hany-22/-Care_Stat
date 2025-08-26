USE Care_Stat;
GO

CREATE TABLE Patients (
    patient_id INT PRIMARY KEY,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50) NOT NULL,
    gender NVARCHAR(10) NOT NULL
        CHECK (gender IN (N'male', N'female')),
    age INT NOT NULL CHECK (age BETWEEN 0 AND 120),
    height_cm DECIMAL(5,2) NULL CHECK (height_cm > 0),
    weight_kg DECIMAL(5,2) NULL CHECK (weight_kg > 0),
    country NVARCHAR(50) NULL,
    city NVARCHAR(50) NULL,
    visits_count INT NOT NULL DEFAULT 0 CHECK (visits_count >= 0)
);