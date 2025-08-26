USE Care_Stat;
GO

CREATE TABLE DoctorDepartment (
    doctor_id INT NOT NULL,
    department_id INT NOT NULL,
    workload_hours_week INT NOT NULL CHECK (workload_hours_week >= 0),

    CONSTRAINT PK_DoctorDepartment PRIMARY KEY (doctor_id, department_id),
    CONSTRAINT FK_DoctorDepartment_Doctor FOREIGN KEY (doctor_id) 
        REFERENCES Doctors(doctor_id),
    CONSTRAINT FK_DoctorDepartment_Department FOREIGN KEY (department_id) 
        REFERENCES Departments(department_id)
);