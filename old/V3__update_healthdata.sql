UPDATE healthdata
SET reading_value = reading_value * 1.05
WHERE reading_type = 'blood_pressure';
