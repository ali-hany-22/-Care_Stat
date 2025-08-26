USE Care_Stat;
GO

CREATE TABLE Visits (
    visit_id INT PRIMARY KEY ,
    patient_id INT NOT NULL,
    visit_date DATE NOT NULL,

    CONSTRAINT FK_Visits_Patients FOREIGN KEY (patient_id) 
        REFERENCES Patients(patient_id)
);