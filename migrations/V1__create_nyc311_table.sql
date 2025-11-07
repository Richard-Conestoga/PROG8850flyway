CREATE TABLE service_requests (
unique_key BIGINT PRIMARY KEY,
created_date DATETIME,
closed_date DATETIME,
agency VARCHAR(20),
complaint_type VARCHAR(100),
descriptor VARCHAR(255),
borough VARCHAR(50),
latitude DECIMAL(10,6),
longitude DECIMAL(10,6)
);