USE Care_Stat;
GO

CREATE TABLE PatientPhones (
    patient_id INT NOT NULL,
    phone NVARCHAR(15) NOT NULL,

    CONSTRAINT PK_PatientPhones PRIMARY KEY (patient_id, phone),
    CONSTRAINT FK_PatientPhones_Patients FOREIGN KEY (patient_id) 
        REFERENCES Patients(patient_id),
    CONSTRAINT CHK_PatientPhone CHECK (LEN(phone) = 11 AND phone NOT LIKE '%[^0-9]%')
);