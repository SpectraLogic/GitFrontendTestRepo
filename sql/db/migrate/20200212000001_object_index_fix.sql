-- Fix object index --
DROP INDEX IF EXISTS ds3.s3_object_bucket_id_name_idx;

CREATE INDEX IF NOT EXISTS s3_object_bucket_id_name_creation_date_id_idx
    ON ds3.s3_object(bucket_id ASC, name ASC, creation_date DESC, id ASC);
