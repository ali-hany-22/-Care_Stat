USE Care_Stat;
GO


CREATE TABLE DoctorPhones (
    doctor_id INT NOT NULL,
    phone NVARCHAR(15) NOT NULL,
    CONSTRAINT PK_DoctorPhones PRIMARY KEY (doctor_id, phone),
    CONSTRAINT FK_DoctorPhones_Doctors FOREIGN KEY (doctor_id) 
        REFERENCES Doctors(doctor_id),
    CONSTRAINT CHK_PhoneLength CHECK (LEN(phone) = 11 AND phone NOT LIKE '%[^0-9]%')
);