USE Care_Stat;
GO

CREATE TABLE Payments (
    payment_id INT PRIMARY KEY,
    patient_id INT NOT NULL,
    appointment_id INT NULL,
    record_id INT NULL,
    department_id INT NULL,
    method NVARCHAR(50) NOT NULL
        CHECK (method IN (N'cash', N'credit_card', N'debit_card', N'insurance', N'online')),
    amount DECIMAL(10,2) NOT NULL CHECK (amount >= 0),
    payment_date DATETIME NOT NULL DEFAULT GETDATE(),
    payment_status NVARCHAR(20) NOT NULL
        CHECK (payment_status IN (N'pending', N'completed', N'failed', N'refunded')),
    transaction_id NVARCHAR(100) UNIQUE NULL,

    CONSTRAINT FK_Payments_Patients FOREIGN KEY (patient_id) 
        REFERENCES Patients(patient_id),
    CONSTRAINT FK_Payments_Appointments FOREIGN KEY (appointment_id) 
        REFERENCES Appointments(appointment_id),
    CONSTRAINT FK_Payments_Records FOREIGN KEY (record_id) 
        REFERENCES Medical_Records(record_id),
    CONSTRAINT FK_Payments_Departments FOREIGN KEY (department_id) 
        REFERENCES Departments(department_id)
);