-- com.spectralogic.s3.common.dao.domain.tape.RawImportTapeDirective --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='tape' AND tablename='raw_import_tape_directive') THEN
  CREATE TABLE tape.raw_import_tape_directive (
    bucket_id        uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
    conflict_resolution_mode shared.import_conflict_resolution_mode NOT NULL,
    data_policy_id   uuid                   NULL       REFERENCES ds3.data_policy ON UPDATE CASCADE,
    id               uuid                   NOT NULL,
    storage_domain_id uuid                  NULL       REFERENCES ds3.storage_domain ON UPDATE CASCADE,
    tape_id          uuid                   NOT NULL   REFERENCES tape.tape ON UPDATE CASCADE ON DELETE CASCADE,
    user_id          uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE,
    verify_data_after_import ds3.blob_store_task_priority NULL,
    verify_data_prior_to_import boolean     NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (tape_id)
  );
CREATE INDEX ON tape.raw_import_tape_directive (bucket_id);
CREATE INDEX ON tape.raw_import_tape_directive (data_policy_id);
CREATE INDEX ON tape.raw_import_tape_directive (storage_domain_id);
CREATE INDEX ON tape.raw_import_tape_directive (user_id);
END IF;
END
$$;