-- Add Job.restore --
CREATE TYPE ds3.job_restore AS ENUM ();
ALTER TYPE ds3.job_restore add value if not exists 'NO';
COMMIT;
ALTER TABLE ds3.job ADD COLUMN restore ds3.job_restore;
UPDATE ds3.job SET restore='NO';
ALTER TABLE ds3.job ALTER COLUMN restore set not null;

-- Add Ds3.dataMigration --
CREATE TABLE ds3.data_migration (
  get_job_id         uuid                NULL REFERENCES ds3.job ON UPDATE CASCADE ON DELETE SET NULL,
  put_job_id         uuid                NULL REFERENCES ds3.job ON UPDATE CASCADE ON DELETE SET NULL,
  in_error           boolean             NOT NULL,
  id                 uuid                NOT NULL,
  PRIMARY KEY (id)
);
CREATE INDEX ON ds3.data_migration (put_job_id);
CREATE INDEX ON ds3.data_migration (get_job_id);

-- Add Ds3.obsoletion --
CREATE TABLE ds3.obsoletion (
  date               timestamp without time zone NULL,
  id                 uuid                NOT NULL,
  PRIMARY KEY (id)
);


-- Remove Tape.blobTape unique constraint --
ALTER TABLE tape.blob_tape DROP CONSTRAINT IF EXISTS blob_tape_blob_id_tape_id_key;

-- Remove Pool.blobPool unique constraint --
ALTER TABLE pool.blob_pool DROP CONSTRAINT IF EXISTS blob_pool_blob_id_pool_id_key;
