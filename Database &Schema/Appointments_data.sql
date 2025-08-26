USE Care_Stat;
GO

CREATE TABLE Appointments (
    appointment_id INT PRIMARY KEY,
    doctor_id INT NOT NULL,
    patient_id INT NOT NULL,
    appointment_date DATETIME NOT NULL,
    notes NVARCHAR(255) NULL,

    CONSTRAINT FK_Appointments_Doctors FOREIGN KEY (doctor_id) 
        REFERENCES Doctors(doctor_id),
    CONSTRAINT FK_Appointments_Patients FOREIGN KEY (patient_id) 
        REFERENCES Patients(patient_id)
);
