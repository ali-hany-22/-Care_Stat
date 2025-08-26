USE Care_Stat;
GO

CREATE TABLE Doctors (
    doctor_id INT PRIMARY KEY ,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    age INT NULL 
        CHECK (age BETWEEN 23 AND 100),
    email NVARCHAR(100) NULL,
    specialization NVARCHAR(50) NULL,
    graduation_year INT NULL
        CHECK (graduation_year BETWEEN 1950 AND 2025),
    university_grade NVARCHAR(20) NOT NULL,
    educational_degree NVARCHAR(20) NOT NULL,
    hire_year INT NULL
        CHECK (hire_year > 0),
    years_of_experience INT NULL
        CHECK (years_of_experience > 0),
    rating_avg DECIMAL(3,2) NULL
        CHECK (rating_avg BETWEEN 0 AND 5),
    salary DECIMAL(10,2) NULL
);
ALTER TABLE Doctors
ADD gender NVARCHAR(10) NOT NULL 
    CHECK (gender IN (N'male', N'female')) DEFAULT N'male';