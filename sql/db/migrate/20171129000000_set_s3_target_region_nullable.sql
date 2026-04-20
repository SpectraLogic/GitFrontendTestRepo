-- Set S3 Target Region to be nullable --

ALTER TABLE target.s3_target ALTER COLUMN region DROP NOT NULL;
