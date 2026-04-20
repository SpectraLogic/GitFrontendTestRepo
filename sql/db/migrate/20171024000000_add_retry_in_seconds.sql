-- Add DataPathBackend.cacheAvailableRetryInSeconds --
ALTER TABLE ds3.data_path_backend ADD COLUMN IF NOT EXISTS cache_available_retry_after_in_seconds INT;
UPDATE ds3.data_path_backend SET cache_available_retry_after_in_seconds=300;
ALTER TABLE ds3.data_path_backend ALTER COLUMN cache_available_retry_after_in_seconds SET NOT NULL;
