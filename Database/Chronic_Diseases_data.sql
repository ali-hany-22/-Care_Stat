USE Care_Stat;
GO

CREATE TABLE ChronicDiseases (
    disease_id INT PRIMARY KEY ,
    disease_name NVARCHAR(100) NOT NULL UNIQUE
);

