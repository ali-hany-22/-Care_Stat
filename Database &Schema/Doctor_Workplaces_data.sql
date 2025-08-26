USE Care_Stat;
GO

CREATE TABLE DoctorWorkplaces (
    doctor_id INT NOT NULL,
    workplace NVARCHAR(100) NOT NULL,
    CONSTRAINT PK_DoctorWorkplaces PRIMARY KEY (doctor_id, workplace),
    CONSTRAINT FK_DoctorWorkplaces_Doctors FOREIGN KEY (doctor_id) 
        REFERENCES Doctors(doctor_id)
);