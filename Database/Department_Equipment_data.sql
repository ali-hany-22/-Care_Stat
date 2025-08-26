USE Care_Stat;
GO

CREATE TABLE Department_Equipment (
    department_id INT NOT NULL,
    equipment_name NVARCHAR(100) NOT NULL,

    CONSTRAINT PK_Department_Equipment PRIMARY KEY (department_id, equipment_name),
    CONSTRAINT FK_Department_Equipment_Department FOREIGN KEY (department_id) 
        REFERENCES Departments(department_id)
);