-- com.spectralogic.s3.common.dao.domain.shared.ImportConflictResolutionMode --
CREATE TYPE shared.import_conflict_resolution_mode AS ENUM ();

-- com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='data_path_backend') THEN
  CREATE TABLE ds3.data_path_backend (
    activated        boolean                NOT NULL,
    allow_new_jobs_to_use_unavailable_pools boolean NOT NULL,
    allow_new_jobs_to_use_unavailable_tape_partitions boolean NOT NULL,
    auto_activate_timeout_in_mins integer   NULL,
    auto_inspect_on_startup boolean         NOT NULL,
    default_import_conflict_resolution_mode shared.import_conflict_resolution_mode NOT NULL,
    id               uuid                   NOT NULL,
    last_heartbeat   timestamp without time zone NOT NULL,
    unavailable_pool_max_job_retry_in_mins integer NOT NULL,
    unavailable_tape_partition_max_job_retry_in_mins integer NOT NULL,

    PRIMARY KEY (id)
  );
END IF;
END
$$;


-- SCHEMA security --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = 'security') THEN
  CREATE SCHEMA security;
END IF;
END
$$;

DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='pool' AND tablename='pool_partition') THEN
  CREATE TABLE pool.pool_partition (
     id                uuid                   NOT NULL,
     name              varchar                NOT NULL,
     type              pool.pool_type         NOT NULL,

     PRIMARY KEY (id),
     UNIQUE (name)
  );
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel --
CREATE TYPE ds3.versioning_level AS ENUM ('NONE');

-- com.spectralogic.s3.common.dao.domain.ds3.DataIsolationLevel --
CREATE TYPE ds3.data_isolation_level AS ENUM ();

-- com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMemberState --
CREATE TYPE ds3.storage_domain_member_state AS ENUM ();

-- com.spectralogic.s3.common.dao.domain.ds3.WritePreferenceLevel --
CREATE TYPE ds3.write_preference_level AS ENUM ();

-- com.spectralogic.s3.common.dao.domain.ds3.LtfsCompatibilityLevel --
CREATE TYPE ds3.ltfs_file_naming_mode AS ENUM ();

-- com.spectralogic.s3.common.dao.domain.ds3.StorageDomain --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='storage_domain') THEN
  CREATE TABLE ds3.storage_domain (
    auto_eject_upon_cron varchar            NULL,
    auto_eject_upon_job_cancellation boolean NOT NULL,
    auto_eject_upon_job_completion boolean  NOT NULL,
    auto_eject_upon_media_full boolean      NOT NULL,
    id               uuid                   NOT NULL,
    ltfs_file_naming ds3.ltfs_file_naming_mode NOT NULL,
    max_tape_fragmentation_percent integer  NOT NULL,
    maximum_auto_verification_frequency_in_days integer NOT NULL,
    media_ejection_allowed boolean          NOT NULL,
    name             varchar                NOT NULL,
    write_optimization ds3.write_optimization NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (name)
  );
END IF;
END
$$;

-- com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='storage_domain_member') THEN
  CREATE TABLE ds3.storage_domain_member (
    pool_partition_id uuid                   NULL       REFERENCES pool.pool_partition ON UPDATE CASCADE,
    id                uuid                   NOT NULL,
    state             ds3.storage_domain_member_state NOT NULL,
    storage_domain_id uuid                   NOT NULL   REFERENCES ds3.storage_domain ON UPDATE CASCADE,
    tape_partition_id uuid                   NULL       REFERENCES tape.tape_partition ON UPDATE CASCADE,
    tape_type         tape.tape_type         NULL,
    write_preference  ds3.write_preference_level NOT NULL,

    PRIMARY KEY (id),
    UNIQUE (storage_domain_id, pool_partition_id),
    UNIQUE (storage_domain_id, tape_partition_id, tape_type)
  );
CREATE INDEX ON ds3.storage_domain_member (pool_partition_id);
CREATE INDEX ON ds3.storage_domain_member (storage_domain_id);
CREATE INDEX ON ds3.storage_domain_member (tape_partition_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.DataPolicy --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='data_policy') THEN
  CREATE TABLE ds3.data_policy (
    checksum_type     security.checksum_type NOT NULL,
    creation_date     timestamp without time zone NOT NULL,
    default_blob_size bigint                 NULL,
    default_get_job_priority ds3.blob_store_task_priority NOT NULL,
    default_put_job_priority ds3.blob_store_task_priority NOT NULL,
    default_verify_job_priority ds3.blob_store_task_priority NOT NULL,
    end_to_end_crc_required boolean          NOT NULL,
    blobbing_enabled  boolean                NOT NULL,
    ltfs_object_naming_allowed boolean NOT NULL,
    id                uuid                   NOT NULL,
    name              varchar                NOT NULL,
    rebuild_priority  ds3.blob_store_task_priority NOT NULL,
    PRIMARY KEY (id), 
    UNIQUE (name)
  );
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleState --
CREATE TYPE ds3.data_persistence_rule_state AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType --
CREATE TYPE ds3.data_persistence_rule_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='data_persistence_rule') THEN
  CREATE TABLE ds3.data_persistence_rule (
    data_policy_id    uuid                   NOT NULL   REFERENCES ds3.data_policy ON UPDATE CASCADE,
    id                uuid                   NOT NULL,
    isolation_level   ds3.data_isolation_level NOT NULL,
    minimum_days_to_retain    integer        NULL,
    state             ds3.data_persistence_rule_state NOT NULL,
    storage_domain_id uuid                   NOT NULL   REFERENCES ds3.storage_domain ON UPDATE CASCADE,
    type              ds3.data_persistence_rule_type NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (data_policy_id, storage_domain_id)
  );
CREATE INDEX ON ds3.data_persistence_rule (data_policy_id);
CREATE INDEX ON ds3.data_persistence_rule (storage_domain_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.DataPolicyAcl --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='data_policy_acl') THEN
  CREATE TABLE ds3.data_policy_acl (
    data_policy_id    uuid                   NULL       REFERENCES ds3.data_policy ON UPDATE CASCADE ON DELETE CASCADE,
    group_id          uuid                   NULL       REFERENCES ds3.group ON UPDATE CASCADE ON DELETE CASCADE,
    id                uuid                   NOT NULL,
    user_id           uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id),
    UNIQUE (data_policy_id, group_id), 
    UNIQUE (data_policy_id, user_id)
  );
CREATE INDEX ON ds3.data_policy_acl (data_policy_id);
CREATE INDEX ON ds3.data_policy_acl (group_id);
CREATE INDEX ON ds3.data_policy_acl (user_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='degraded_blob') THEN
  CREATE TABLE ds3.degraded_blob (
    bucket_id           uuid                NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
    blob_id             uuid                NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    id                  uuid                NOT NULL,
    persistence_rule_id uuid                NOT NULL   REFERENCES ds3.data_persistence_rule ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (persistence_rule_id, blob_id)
  );
CREATE INDEX ON ds3.degraded_blob (blob_id);
CREATE INDEX ON ds3.degraded_blob (persistence_rule_id);
CREATE INDEX ON ds3.degraded_blob (bucket_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.ds3.JobChunkPersistenceTarget --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='job_chunk_persistence_target') THEN
  CREATE TABLE ds3.job_chunk_persistence_target (
    chunk_id          uuid                   NOT NULL   REFERENCES ds3.job_chunk ON UPDATE CASCADE ON DELETE CASCADE,
    id                uuid                   NOT NULL,
    storage_domain_id uuid                   NOT NULL   REFERENCES ds3.storage_domain ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (chunk_id, storage_domain_id)
  );
CREATE INDEX ON ds3.job_chunk_persistence_target (chunk_id);
CREATE INDEX ON ds3.job_chunk_persistence_target (storage_domain_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailureType --
CREATE TYPE ds3.storage_domain_failure_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailure --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='storage_domain_failure') THEN
  CREATE TABLE ds3.storage_domain_failure (
    date              timestamp without time zone NOT NULL,
    error_message     varchar                NULL,
    id                uuid                   NOT NULL,
    storage_domain_id uuid                   NOT NULL   REFERENCES ds3.storage_domain ON UPDATE CASCADE ON DELETE CASCADE,
    type              ds3.storage_domain_failure_type NOT NULL,

    PRIMARY KEY (id)
  );
CREATE INDEX ON ds3.storage_domain_failure (storage_domain_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.notification.StorageDomainFailureNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='storage_domain_failure_notification_registration') THEN
  CREATE TABLE notification.storage_domain_failure_notification_registration (
    creation_date     timestamp without time zone NOT NULL,
    format            http.http_response_format_type NOT NULL,
    id                uuid                   NOT NULL,
    last_http_response_code integer          NULL,
    last_notification timestamp without time zone NULL,
    naming_convention lang.naming_convention_type NOT NULL,
    notification_end_point varchar           NOT NULL,
    notification_http_method http.request_type NOT NULL,
    number_of_failures_since_last_success integer NOT NULL,
    user_id           uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id)
  );
CREATE INDEX ON notification.storage_domain_failure_notification_registration (user_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.tape.TapeDensityDirective --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='tape' AND tablename='tape_density_directive') THEN
  CREATE TABLE tape.tape_density_directive (
    density          tape.tape_drive_type   NOT NULL,
    id               uuid                   NOT NULL,
    partition_id     uuid                   NOT NULL   REFERENCES tape.tape_partition ON UPDATE CASCADE ON DELETE CASCADE,
    tape_type        tape.tape_type         NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (partition_id, tape_type)
  );
CREATE INDEX ON tape.tape_density_directive (partition_id);
END IF;
END
$$;

-- com.spectralogic.s3.common.dao.domain.pool.ImportPoolDirective --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='pool' AND tablename='import_pool_directive') THEN
  CREATE TABLE pool.import_pool_directive (
    conflict_resolution_mode shared.import_conflict_resolution_mode NOT NULL,
    data_policy_id   uuid                   NULL       REFERENCES ds3.data_policy ON UPDATE CASCADE,
    id               uuid                   NOT NULL,
    pool_id          uuid                   NOT NULL   REFERENCES pool.pool ON UPDATE CASCADE ON DELETE CASCADE,
    storage_domain_id uuid                  NULL       REFERENCES ds3.storage_domain ON UPDATE CASCADE,
    user_id          uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (pool_id)
  );
CREATE INDEX ON pool.import_pool_directive (data_policy_id);
CREATE INDEX ON pool.import_pool_directive (storage_domain_id);
CREATE INDEX ON pool.import_pool_directive (user_id);
END IF;
END
$$;

-- com.spectralogic.s3.common.dao.domain.tape.ImportTapeDirective --
DROP TABLE tape.import_tape_directive;
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='tape' AND tablename='import_tape_directive') THEN
  CREATE TABLE tape.import_tape_directive (
    conflict_resolution_mode shared.import_conflict_resolution_mode NOT NULL,
    data_policy_id   uuid                   NULL       REFERENCES ds3.data_policy ON UPDATE CASCADE,
    id               uuid                   NOT NULL,
    storage_domain_id uuid                  NULL       REFERENCES ds3.storage_domain ON UPDATE CASCADE,
    tape_id          uuid                   NOT NULL   REFERENCES tape.tape ON UPDATE CASCADE ON DELETE CASCADE,
    user_id          uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (tape_id)
  );
CREATE INDEX ON tape.import_tape_directive (data_policy_id);
CREATE INDEX ON tape.import_tape_directive (storage_domain_id);
CREATE INDEX ON tape.import_tape_directive (user_id);
END IF;
END
$$;




-- com.spectralogic.s3.common.dao.domain.ds3.SystemFailureType --
CREATE TYPE ds3.system_failure_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.SystemFailure --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='system_failure') THEN
  CREATE TABLE ds3.system_failure (
    date             timestamp without time zone NOT NULL,
    error_message    varchar                NULL,
    id               uuid                   NOT NULL,
    type             ds3.system_failure_type NOT NULL,

    PRIMARY KEY (id)
  );
END IF;
END
$$;





-- com.spectralogic.s3.common.dao.domain.notification.SystemFailureNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='system_failure_notification_registration') THEN
  CREATE TABLE notification.system_failure_notification_registration (
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
CREATE INDEX ON notification.system_failure_notification_registration (user_id);
END IF;
END
$$;




ALTER TABLE tape.tape ADD COLUMN take_ownership_pending boolean;
UPDATE tape.tape SET take_ownership_pending=false;
ALTER TABLE tape.tape ALTER COLUMN take_ownership_pending set NOT NULL;

ALTER TABLE tape.tape ADD COLUMN assigned_to_storage_domain boolean;
UPDATE tape.tape SET assigned_to_storage_domain=false;
UPDATE tape.tape SET assigned_to_storage_domain=true where assigned_to_bucket=true;
ALTER TABLE tape.tape ALTER COLUMN assigned_to_storage_domain set NOT NULL;

ALTER TABLE tape.tape ADD COLUMN storage_domain_id uuid NULL REFERENCES ds3.storage_domain ON UPDATE CASCADE ON DELETE SET NULL;
CREATE INDEX ON tape.tape (storage_domain_id);

ALTER TABLE ds3.user ADD COLUMN default_data_policy_id uuid NULL REFERENCES ds3.data_policy ON UPDATE CASCADE ON DELETE SET NULL;
CREATE INDEX ON ds3.user (default_data_policy_id);

-- This table will be updated to add the not null constraint once data policies have been created for existing buckets
ALTER TABLE ds3.bucket ADD COLUMN data_policy_id uuid NULL REFERENCES ds3.data_policy ON UPDATE CASCADE;
CREATE INDEX ON ds3.bucket (data_policy_id);

ALTER TABLE ds3.job_chunk ADD COLUMN read_from_pool_id uuid NULL REFERENCES pool.pool ON UPDATE CASCADE ON DELETE SET NULL;
ALTER TABLE ds3.job_chunk ADD COLUMN read_from_tape_id uuid NULL REFERENCES tape.tape ON UPDATE CASCADE ON DELETE SET NULL;
CREATE INDEX ON ds3.job_chunk (read_from_pool_id);
CREATE INDEX ON ds3.job_chunk (read_from_tape_id);

ALTER TABLE pool.pool ADD COLUMN assigned_to_storage_domain boolean NOT NULL;
ALTER TABLE pool.pool ADD COLUMN bucket_id uuid NULL REFERENCES ds3.bucket ON UPDATE CASCADE;
ALTER TABLE pool.pool ADD COLUMN storage_domain_id uuid NULL REFERENCES ds3.storage_domain ON UPDATE CASCADE ON DELETE SET NULL;
ALTER TABLE pool.pool ADD COLUMN partition_id uuid NULL REFERENCES pool.pool_partition ON UPDATE CASCADE ON DELETE SET NULL;
ALTER TABLE pool.pool ADD COLUMN total_capacity bigint NOT NULL;
CREATE INDEX ON pool.pool (bucket_id);
CREATE INDEX ON pool.pool (partition_id);
CREATE INDEX ON pool.pool (storage_domain_id);

ALTER TABLE ds3.s3_object DROP CONSTRAINT s3_object_bucket_id_name_key;
CREATE INDEX ON ds3.s3_object (bucket_id, name);
ALTER TABLE ds3.s3_object ADD CONSTRAINT s3_object_bucket_id_name_version_idx UNIQUE (bucket_id, name, version);
