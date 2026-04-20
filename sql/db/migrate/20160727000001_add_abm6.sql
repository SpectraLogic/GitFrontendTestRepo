-- Add Job.implicitJobIdResolutionEnabled --
alter table ds3.job add column implicit_job_id_resolution boolean;
update ds3.job set implicit_job_id_resolution = true;
alter table ds3.job alter column implicit_job_id_resolution set not null;


-- Add Job.verify_after_write --
alter table ds3.job add column verify_after_write boolean;
update ds3.job set verify_after_write = false;
alter table ds3.job alter column verify_after_write set not null;


-- Add DataPolicy.default_verify_after_write --
alter table ds3.data_policy add column default_verify_after_write boolean;
update ds3.data_policy set default_verify_after_write = false;
alter table ds3.data_policy alter column default_verify_after_write set not null;


-- Ensure TargetState.LIMITED_ACCESS is unused since we no longer support it --
update target.ds3_target set state='OFFLINE' where state='LIMITED_ACCESS';


-- Column renames for multiple cloud out types --
alter table ds3.degraded_blob rename column replication_rule_id to ds3_replication_rule_id;
alter table ds3.data_replication_rule rename column ds3_target_id to target_id;
alter table ds3.data_replication_rule rename column ds3_target_data_policy to target_data_policy;
alter table target.blob_target rename column ds3_target_id to target_id;
alter table target.suspect_blob_target rename column ds3_target_id to target_id;


-- Table renames for multiple cloud out types --
alter table ds3.data_replication_rule rename to ds3_data_replication_rule;
alter table target.blob_target rename to blob_ds3_target;
alter table target.suspect_blob_target rename to suspect_blob_ds3_target;
alter type target.ds3_target_failure_type rename to target_failure_type;
alter type target.target_read_preference rename to target_read_preference_type;
alter type ds3.data_persistence_rule_state rename to data_placement_rule_state;


-- Add replicate_deletes column to ds3.data_replication_rule --
alter table ds3.ds3_data_replication_rule add column replicate_deletes boolean;
update ds3.ds3_data_replication_rule set replicate_deletes = (select always_replicate_deletes from ds3.data_policy where id=ds3.ds3_data_replication_rule.data_policy_id);
alter table ds3.ds3_data_replication_rule alter column replicate_deletes set not null;

-- Remove always_replicate_delets from ds3.data_policy --
alter table ds3.data_policy drop column always_replicate_deletes;

-- Remove ltfs_object_naming_allowed from ds3.data_policy --
alter table ds3.data_policy drop column ltfs_object_naming_allowed;


-- com.spectralogic.s3.common.dao.domain.ds3.FeatureKeyType --
CREATE TYPE ds3.feature_key_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.FeatureKey --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='feature_key') THEN
  CREATE TABLE ds3.feature_key (
    current_value    bigint                 NULL,
    error_message    varchar                NULL,
    expiration_date  timestamp without time zone NULL,
    id               uuid                   NOT NULL,
    key              ds3.feature_key_type   NOT NULL,
    limit_value      bigint                 NULL,

    PRIMARY KEY (id), 
    UNIQUE (key)
  );
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.target.AzureTarget --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='azure_target') THEN
  CREATE TABLE target.azure_target (
    account_key      varchar                NOT NULL,
    account_name     varchar                NOT NULL,
    auto_verify_frequency_in_days integer   NULL,
    cloud_bucket_prefix varchar             NOT NULL,
    cloud_bucket_suffix varchar             NOT NULL,
    default_read_preference target.target_read_preference_type NOT NULL,
    https            boolean                NOT NULL,
    id               uuid                   NOT NULL,
    last_fully_verified timestamp without time zone NULL,
    name             varchar                NOT NULL,
    permit_going_out_of_sync boolean        NOT NULL,
    quiesced         shared.quiesced        NOT NULL,
    state            target.target_state    NOT NULL,

    PRIMARY KEY (id)
  );
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.ds3.S3Region --
CREATE TYPE ds3.s3_region AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.target.S3Target --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='s3_target') THEN
  CREATE TABLE target.s3_target (
    access_key       varchar                NOT NULL,
    auto_verify_frequency_in_days integer   NULL,
    cloud_bucket_prefix varchar             NOT NULL,
    cloud_bucket_suffix varchar             NOT NULL,
    data_path_end_point varchar             NULL,
    default_read_preference target.target_read_preference_type NOT NULL,
    https            boolean                NOT NULL,
    id               uuid                   NOT NULL,
    last_fully_verified timestamp without time zone NULL,
    name             varchar                NOT NULL,
    offline_data_staging_window_in_tb integer NOT NULL,
    permit_going_out_of_sync boolean        NOT NULL,
    proxy_domain     varchar                NULL,
    proxy_host       varchar                NULL,
    proxy_password   varchar                NULL,
    proxy_port       integer                NULL,
    proxy_username   varchar                NULL,
    quiesced         shared.quiesced        NOT NULL,
    region           ds3.s3_region          NOT NULL,
    secret_key       varchar                NOT NULL,
    staged_data_expiration_in_days integer  NOT NULL,
    state            target.target_state    NOT NULL,

    PRIMARY KEY (id)
  );
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.ds3.S3InitialDataPlacementPolicy --
CREATE TYPE ds3.s3_initial_data_placement_policy AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='s3_data_replication_rule') THEN
  CREATE TABLE ds3.s3_data_replication_rule (
    data_policy_id   uuid                   NOT NULL   REFERENCES ds3.data_policy ON UPDATE CASCADE,
    id               uuid                   NOT NULL,
    initial_data_placement ds3.s3_initial_data_placement_policy NOT NULL,
    max_blob_part_size_in_bytes bigint      NOT NULL,
    replicate_deletes boolean               NOT NULL,
    state            ds3.data_placement_rule_state NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.s3_target ON UPDATE CASCADE,
    type             ds3.data_replication_rule_type NOT NULL,

    PRIMARY KEY (id)
  );
CREATE INDEX ON ds3.s3_data_replication_rule (data_policy_id);
CREATE INDEX ON ds3.s3_data_replication_rule (target_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='azure_data_replication_rule') THEN
  CREATE TABLE ds3.azure_data_replication_rule (
    data_policy_id   uuid                   NOT NULL   REFERENCES ds3.data_policy ON UPDATE CASCADE,
    id               uuid                   NOT NULL,
    max_blob_part_size_in_bytes bigint      NOT NULL,
    replicate_deletes boolean               NOT NULL,
    state            ds3.data_placement_rule_state NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.azure_target ON UPDATE CASCADE,
    type             ds3.data_replication_rule_type NOT NULL,

    PRIMARY KEY (id)
  );
CREATE INDEX ON ds3.azure_data_replication_rule (data_policy_id);
CREATE INDEX ON ds3.azure_data_replication_rule (target_id);
END IF;
END
$$;




-- com.spectralogic.s3.common.dao.domain.notification.AzureTargetFailureNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='azure_target_failure_notification_registration') THEN
  CREATE TABLE notification.azure_target_failure_notification_registration (
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
CREATE INDEX ON notification.azure_target_failure_notification_registration (user_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.notification.S3TargetFailureNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='s3_target_failure_notification_registration') THEN
  CREATE TABLE notification.s3_target_failure_notification_registration (
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
CREATE INDEX ON notification.s3_target_failure_notification_registration (user_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.target.S3TargetBucketName --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='s3_target_bucket_name') THEN
  CREATE TABLE target.s3_target_bucket_name (
    bucket_id        uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
    id               uuid                   NOT NULL,
    name             varchar                NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.s3_target ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (bucket_id, target_id)
  );
CREATE INDEX ON target.s3_target_bucket_name (bucket_id);
CREATE INDEX ON target.s3_target_bucket_name (target_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.target.AzureTargetBucketName --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='azure_target_bucket_name') THEN
  CREATE TABLE target.azure_target_bucket_name (
    bucket_id        uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
    id               uuid                   NOT NULL,
    name             varchar                NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.azure_target ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (bucket_id, target_id)
  );
CREATE INDEX ON target.azure_target_bucket_name (bucket_id);
CREATE INDEX ON target.azure_target_bucket_name (target_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.target.AzureTargetFailure --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='azure_target_failure') THEN
  CREATE TABLE target.azure_target_failure (
    date             timestamp without time zone NOT NULL,
    error_message    varchar                NULL,
    id               uuid                   NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.azure_target ON UPDATE CASCADE ON DELETE CASCADE,
    type             target.target_failure_type NOT NULL,

    PRIMARY KEY (id)
  );
CREATE INDEX ON target.azure_target_failure (target_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.target.AzureTargetReadPreference --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='azure_target_read_preference') THEN
  CREATE TABLE target.azure_target_read_preference (
    bucket_id        uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
    id               uuid                   NOT NULL,
    read_preference  target.target_read_preference_type NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.azure_target ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (target_id, bucket_id)
  );
CREATE INDEX ON target.azure_target_read_preference (bucket_id);
CREATE INDEX ON target.azure_target_read_preference (target_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.target.BlobS3Target --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='blob_s3_target') THEN
  CREATE TABLE target.blob_s3_target (
    blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    id               uuid                   NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.s3_target ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (blob_id, target_id)
  );
CREATE INDEX ON target.blob_s3_target (blob_id);
CREATE INDEX ON target.blob_s3_target (target_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='blob_azure_target') THEN
  CREATE TABLE target.blob_azure_target (
    blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    id               uuid                   NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.azure_target ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (blob_id, target_id)
  );
CREATE INDEX ON target.blob_azure_target (blob_id);
CREATE INDEX ON target.blob_azure_target (target_id);
END IF;
END
$$;





-- com.spectralogic.s3.common.dao.domain.target.S3TargetFailure --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='s3_target_failure') THEN
  CREATE TABLE target.s3_target_failure (
    date             timestamp without time zone NOT NULL,
    error_message    varchar                NULL,
    id               uuid                   NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.s3_target ON UPDATE CASCADE ON DELETE CASCADE,
    type             target.target_failure_type NOT NULL,

    PRIMARY KEY (id)
  );
CREATE INDEX ON target.s3_target_failure (target_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.target.S3TargetReadPreference --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='s3_target_read_preference') THEN
  CREATE TABLE target.s3_target_read_preference (
    bucket_id        uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
    id               uuid                   NOT NULL,
    read_preference  target.target_read_preference_type NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.s3_target ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (target_id, bucket_id)
  );
CREATE INDEX ON target.s3_target_read_preference (bucket_id);
CREATE INDEX ON target.s3_target_read_preference (target_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='suspect_blob_azure_target') THEN
  CREATE TABLE target.suspect_blob_azure_target (
    blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    id               uuid                   NOT NULL   REFERENCES target.blob_azure_target ON UPDATE CASCADE ON DELETE CASCADE,
    target_id        uuid                   NOT NULL   REFERENCES target.azure_target ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (blob_id, target_id)
  );
CREATE INDEX ON target.suspect_blob_azure_target (blob_id);
CREATE INDEX ON target.suspect_blob_azure_target (target_id);
END IF;
END
$$;




-- com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='suspect_blob_s3_target') THEN
  CREATE TABLE target.suspect_blob_s3_target (
    blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    id               uuid                   NOT NULL   REFERENCES target.blob_s3_target ON UPDATE CASCADE ON DELETE CASCADE,
    target_id        uuid                   NOT NULL   REFERENCES target.s3_target ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (blob_id, target_id)
  );
CREATE INDEX ON target.suspect_blob_s3_target (blob_id);
CREATE INDEX ON target.suspect_blob_s3_target (target_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob --
DROP TABLE ds3.degraded_blob;
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='degraded_blob') THEN
  CREATE TABLE ds3.degraded_blob (
    azure_replication_rule_id uuid          NULL       REFERENCES ds3.azure_data_replication_rule ON UPDATE CASCADE ON DELETE CASCADE,
    blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    bucket_id        uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
    ds3_replication_rule_id uuid            NULL       REFERENCES ds3.ds3_data_replication_rule ON UPDATE CASCADE ON DELETE CASCADE,
    id               uuid                   NOT NULL,
    persistence_rule_id uuid                NULL       REFERENCES ds3.data_persistence_rule ON UPDATE CASCADE ON DELETE CASCADE,
    s3_replication_rule_id uuid             NULL       REFERENCES ds3.s3_data_replication_rule ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (ds3_replication_rule_id, blob_id), 
    UNIQUE (persistence_rule_id, blob_id)
  );
CREATE INDEX ON ds3.degraded_blob (azure_replication_rule_id);
CREATE INDEX ON ds3.degraded_blob (blob_id);
CREATE INDEX ON ds3.degraded_blob (bucket_id);
CREATE INDEX ON ds3.degraded_blob (ds3_replication_rule_id);
CREATE INDEX ON ds3.degraded_blob (persistence_rule_id);
CREATE INDEX ON ds3.degraded_blob (s3_replication_rule_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.JobChunk --
DROP TABLE ds3.job_entry;
DROP TABLE ds3.job_chunk_persistence_target;
DROP TABLE ds3.job_chunk_ds3_target;
DROP TABLE ds3.job_chunk;
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='job_chunk') THEN
  CREATE TABLE ds3.job_chunk (
    blob_store_state ds3.job_chunk_blob_store_state NOT NULL,
    chunk_number     integer                NOT NULL,
    id               uuid                   NOT NULL,
    job_id           uuid                   NOT NULL   REFERENCES ds3.job ON UPDATE CASCADE ON DELETE CASCADE,
    node_id          uuid                   NULL       REFERENCES ds3.node ON UPDATE CASCADE ON DELETE SET NULL,
    pending_target_commit boolean           NOT NULL,
    read_from_azure_target_id uuid          NULL       REFERENCES target.azure_target ON UPDATE CASCADE ON DELETE SET NULL,
    read_from_ds3_target_id uuid            NULL       REFERENCES target.ds3_target ON UPDATE CASCADE ON DELETE SET NULL,
    read_from_pool_id uuid                  NULL       REFERENCES pool.pool ON UPDATE CASCADE ON DELETE SET NULL,
    read_from_s3_target_id uuid             NULL       REFERENCES target.s3_target ON UPDATE CASCADE ON DELETE SET NULL,
    read_from_tape_id uuid                  NULL       REFERENCES tape.tape ON UPDATE CASCADE ON DELETE SET NULL,

    PRIMARY KEY (id), 
    UNIQUE (chunk_number, job_id)
  );
CREATE INDEX ON ds3.job_chunk (blob_store_state);
CREATE INDEX ON ds3.job_chunk (job_id);
CREATE INDEX ON ds3.job_chunk (node_id);
CREATE INDEX ON ds3.job_chunk (read_from_azure_target_id);
CREATE INDEX ON ds3.job_chunk (read_from_ds3_target_id);
CREATE INDEX ON ds3.job_chunk (read_from_pool_id);
CREATE INDEX ON ds3.job_chunk (read_from_s3_target_id);
CREATE INDEX ON ds3.job_chunk (read_from_tape_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.JobEntry --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='job_entry') THEN
  CREATE TABLE ds3.job_entry (
    blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    chunk_id         uuid                   NOT NULL   REFERENCES ds3.job_chunk ON UPDATE CASCADE ON DELETE CASCADE,
    id               uuid                   NOT NULL,
    job_id           uuid                   NOT NULL   REFERENCES ds3.job ON UPDATE CASCADE ON DELETE CASCADE,
    order_index      integer                NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (job_id, blob_id), 
    UNIQUE (order_index, chunk_id)
  );
CREATE INDEX ON ds3.job_entry (blob_id);
CREATE INDEX ON ds3.job_entry (chunk_id);
CREATE INDEX ON ds3.job_entry (job_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.JobChunkPersistenceTarget --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='job_chunk_persistence_target') THEN
  CREATE TABLE ds3.job_chunk_persistence_target (
    chunk_id         uuid                   NOT NULL   REFERENCES ds3.job_chunk ON UPDATE CASCADE ON DELETE CASCADE,
    done             boolean                NOT NULL,
    id               uuid                   NOT NULL,
    storage_domain_id uuid                  NOT NULL   REFERENCES ds3.storage_domain ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (chunk_id, storage_domain_id)
  );
CREATE INDEX ON ds3.job_chunk_persistence_target (chunk_id);
CREATE INDEX ON ds3.job_chunk_persistence_target (storage_domain_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.JobChunkDs3Target --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='job_chunk_ds3_target') THEN
  CREATE TABLE ds3.job_chunk_ds3_target (
    chunk_id         uuid                   NOT NULL   REFERENCES ds3.job_chunk ON UPDATE CASCADE ON DELETE CASCADE,
    committed        boolean                NOT NULL,
    done             boolean                NOT NULL,
    id               uuid                   NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.ds3_target ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (chunk_id, target_id)
  );
CREATE INDEX ON ds3.job_chunk_ds3_target (chunk_id);
CREATE INDEX ON ds3.job_chunk_ds3_target (target_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.ds3.JobChunkAzureTarget --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='job_chunk_azure_target') THEN
  CREATE TABLE ds3.job_chunk_azure_target (
    chunk_id         uuid                   NOT NULL   REFERENCES ds3.job_chunk ON UPDATE CASCADE ON DELETE CASCADE,
    done             boolean                NOT NULL,
    id               uuid                   NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.azure_target ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (chunk_id, target_id)
  );
CREATE INDEX ON ds3.job_chunk_azure_target (chunk_id);
CREATE INDEX ON ds3.job_chunk_azure_target (target_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.JobChunkS3Target --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='job_chunk_s3_target') THEN
  CREATE TABLE ds3.job_chunk_s3_target (
    chunk_id         uuid                   NOT NULL   REFERENCES ds3.job_chunk ON UPDATE CASCADE ON DELETE CASCADE,
    done             boolean                NOT NULL,
    id               uuid                   NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.s3_target ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (chunk_id, target_id)
  );
CREATE INDEX ON ds3.job_chunk_s3_target (chunk_id);
CREATE INDEX ON ds3.job_chunk_s3_target (target_id);
END IF;
END
$$;



-- SCHEMA temp --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = 'temp') THEN
  CREATE SCHEMA temp;
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.temp.BlobAzureTargetToVerify --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='temp' AND tablename='blob_azure_target_to_verify') THEN
  CREATE TABLE temp.blob_azure_target_to_verify (
    blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    id               uuid                   NOT NULL   REFERENCES target.blob_azure_target ON UPDATE CASCADE ON DELETE CASCADE,
    target_id        uuid                   NOT NULL   REFERENCES target.azure_target ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (blob_id, target_id)
  );
CREATE INDEX ON temp.blob_azure_target_to_verify (blob_id);
CREATE INDEX ON temp.blob_azure_target_to_verify (target_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.temp.BlobS3TargetToVerify --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='temp' AND tablename='blob_s3_target_to_verify') THEN
  CREATE TABLE temp.blob_s3_target_to_verify (
    blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    id               uuid                   NOT NULL   REFERENCES target.blob_s3_target ON UPDATE CASCADE ON DELETE CASCADE,
    target_id        uuid                   NOT NULL   REFERENCES target.s3_target ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (blob_id, target_id)
  );
CREATE INDEX ON temp.blob_s3_target_to_verify (blob_id);
CREATE INDEX ON temp.blob_s3_target_to_verify (target_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.target.ImportAzureTargetDirective --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='import_azure_target_directive') THEN
  CREATE TABLE target.import_azure_target_directive (
    cloud_bucket_name varchar               NULL,
    conflict_resolution_mode shared.import_conflict_resolution_mode NOT NULL,
    data_policy_id   uuid                   NULL       REFERENCES ds3.data_policy ON UPDATE CASCADE,
    id               uuid                   NOT NULL,
    priority         ds3.blob_store_task_priority NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.azure_target ON UPDATE CASCADE ON DELETE CASCADE,
    user_id          uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (target_id)
  );
CREATE INDEX ON target.import_azure_target_directive (data_policy_id);
CREATE INDEX ON target.import_azure_target_directive (user_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.target.ImportS3TargetDirective --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='import_s3_target_directive') THEN
  CREATE TABLE target.import_s3_target_directive (
    cloud_bucket_name varchar               NULL,
    conflict_resolution_mode shared.import_conflict_resolution_mode NOT NULL,
    data_policy_id   uuid                   NULL       REFERENCES ds3.data_policy ON UPDATE CASCADE,
    id               uuid                   NOT NULL,
    priority         ds3.blob_store_task_priority NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.s3_target ON UPDATE CASCADE ON DELETE CASCADE,
    user_id          uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (target_id)
  );
CREATE INDEX ON target.import_s3_target_directive (data_policy_id);
CREATE INDEX ON target.import_s3_target_directive (user_id);
END IF;
END
$$;