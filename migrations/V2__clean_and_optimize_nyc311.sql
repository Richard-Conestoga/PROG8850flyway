DELETE FROM service_requests WHERE complaint_type IS NULL;
CREATE INDEX idx_borough ON service_requests(borough);
CREATE INDEX idx_complaint_type ON service_requests(complaint_type);
