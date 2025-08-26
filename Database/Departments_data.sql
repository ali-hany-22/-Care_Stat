USE Care_Stat;
GO

CREATE TABLE Departments (
    department_id INT PRIMARY KEY ,
    department_name NVARCHAR(100) NOT NULL,
    department_code NVARCHAR(20) UNIQUE NOT NULL,
    head_doctor_id INT NULL,
    current_occupancy INT DEFAULT 0 CHECK (current_occupancy >= 0),
    max_capacity INT NOT NULL CHECK (max_capacity > 0),
    num_staff INT NOT NULL CHECK (num_staff >= 0),
    working_hours NVARCHAR(50) NULL,
    emergency_support BIT NOT NULL DEFAULT 0,

    CONSTRAINT FK_Departments_HeadDoctor FOREIGN KEY (head_doctor_id) 
        REFERENCES Doctors(doctor_id)
);

