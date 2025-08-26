USE Care_Stat;
GO

CREATE TABLE Medical_Records (
    record_id INT PRIMARY KEY ,
    patient_id INT NOT NULL,
    doctor_id INT NOT NULL,
    department_id INT NOT NULL,
    diagnosis NVARCHAR(255) NOT NULL,
    severity_level NVARCHAR(20) NOT NULL
        CHECK (severity_level IN (N'low', N'moderate', N'high', N'critical')),
    prescription_cost DECIMAL(10,2) NOT NULL CHECK (prescription_cost >= 0),
    record_date DATE NOT NULL DEFAULT GETDATE(),

    CONSTRAINT FK_MedicalRecords_Patients FOREIGN KEY (patient_id) 
        REFERENCES Patients(patient_id),
    CONSTRAINT FK_MedicalRecords_Doctors FOREIGN KEY (doctor_id) 
        REFERENCES Doctors(doctor_id),
    CONSTRAINT FK_MedicalRecords_Departments FOREIGN KEY (department_id) 
        REFERENCES Departments(department_id)
);