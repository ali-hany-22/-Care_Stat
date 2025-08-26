USE Care_Stat;
GO

CREATE TABLE PatientChronicDiseases (
    patient_id INT NOT NULL,
    disease_id INT NOT NULL,
    has_disease BIT NOT NULL DEFAULT 0,

    CONSTRAINT PK_PatientChronicDiseases PRIMARY KEY (patient_id, disease_id),
    CONSTRAINT FK_PatientChronicDiseases_Patients FOREIGN KEY (patient_id) 
        REFERENCES Patients(patient_id),
    CONSTRAINT FK_PatientChronicDiseases_Diseases FOREIGN KEY (disease_id) 
        REFERENCES ChronicDiseases(disease_id)
);