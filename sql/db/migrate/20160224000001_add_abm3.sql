alter table ds3.storage_domain add column auto_eject_media_full_threshold bigint;

alter table pool.import_pool_directive add column verify_data_after_import ds3.blob_store_task_priority;
alter table pool.import_pool_directive add column verify_data_prior_to_import boolean;
update pool.import_pool_directive set verify_data_prior_to_import = true;
alter table pool.import_pool_directive alter column verify_data_prior_to_import set not null;

alter table tape.import_tape_directive add column verify_data_after_import ds3.blob_store_task_priority;
alter table tape.import_tape_directive add column verify_data_prior_to_import boolean;
update tape.import_tape_directive set verify_data_prior_to_import = true;
alter table tape.import_tape_directive alter column verify_data_prior_to_import set not null;

ALTER TABLE ds3.job ADD COLUMN minimize_spanning_across_media boolean NULL;
UPDATE ds3.job set minimize_spanning_across_media = false;
ALTER TABLE ds3.job alter column minimize_spanning_across_media set not null;

ALTER TABLE ds3.data_policy ADD COLUMN always_force_put_job_creation boolean NULL;
UPDATE ds3.data_policy set always_force_put_job_creation = false;
ALTER TABLE ds3.data_policy alter column always_force_put_job_creation set not null;

ALTER TABLE ds3.data_policy ADD COLUMN always_minimize_spanning_across_media boolean NULL;
UPDATE ds3.data_policy set always_minimize_spanning_across_media = false;
ALTER TABLE ds3.data_policy alter column always_minimize_spanning_across_media set not null;

alter table ds3.canceled_job add column canceled_due_to_timeout boolean;
UPDATE ds3.canceled_job set canceled_due_to_timeout = false;
ALTER TABLE ds3.canceled_job alter column canceled_due_to_timeout set not null;

ALTER TABLE ds3.storage_domain alter column maximum_auto_verification_frequency_in_days DROP NOT NULL;
UPDATE ds3.storage_domain set maximum_auto_verification_frequency_in_days = null;


-- com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='tape' AND tablename='suspect_blob_tape') THEN
  CREATE TABLE tape.suspect_blob_tape (
    blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    id               uuid                   NOT NULL   REFERENCES tape.blob_tape ON UPDATE CASCADE ON DELETE CASCADE,
    order_index      integer                NOT NULL,
    tape_id          uuid                   NOT NULL   REFERENCES tape.tape ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (blob_id, tape_id), 
    UNIQUE (order_index, tape_id)
  );
CREATE INDEX ON tape.suspect_blob_tape (blob_id);
CREATE INDEX ON tape.suspect_blob_tape (tape_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='pool' AND tablename='suspect_blob_pool') THEN
  CREATE TABLE pool.suspect_blob_pool (
    blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    bucket_id        uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
    date_written     timestamp without time zone NOT NULL,
    id               uuid                   NOT NULL   REFERENCES pool.blob_pool ON UPDATE CASCADE ON DELETE CASCADE,
    last_accessed    timestamp without time zone NOT NULL,
    pool_id          uuid                   NOT NULL   REFERENCES pool.pool ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (blob_id, pool_id)
  );
CREATE INDEX ON pool.suspect_blob_pool (blob_id);
CREATE INDEX ON pool.suspect_blob_pool (bucket_id);
CREATE INDEX ON pool.suspect_blob_pool (pool_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.target.SuspectBlobTarget --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='suspect_blob_target') THEN
  CREATE TABLE target.suspect_blob_target (
    blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    ds3_target_id    uuid                   NOT NULL   REFERENCES target.ds3_target ON UPDATE CASCADE,
    id               uuid                   NOT NULL   REFERENCES target.blob_target ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (blob_id, ds3_target_id)
  );
CREATE INDEX ON target.suspect_blob_target (blob_id);
CREATE INDEX ON target.suspect_blob_target (ds3_target_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.notification.JobCreationFailedNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='job_creation_failed_notification_registration') THEN
  CREATE TABLE notification.job_creation_failed_notification_registration (
    creation_date    timestamp without time zone NOT NULL,
    format           http.http_response_format_type NOT NULL,
    id               uuid                   NOT NULL,
    last_failure     varchar                NULL,
    last_http_response_code integer         NULL,
    last_notification timestamp without time zone NULL,
    naming_convention lang.naming_convention_type NOT NULL,
    notification_end_point varchar          NOT NULL,
    notification_http_method http.request_type NOT NULL,
    number_of_failures_since_last_success integer NOT NULL,
    user_id          uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id)
  );
CREATE INDEX ON notification.job_creation_failed_notification_registration (user_id);
END IF;
END
$$;