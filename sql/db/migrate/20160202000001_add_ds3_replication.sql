-- SCHEMA replication --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = 'target') THEN
  CREATE SCHEMA target;
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.target.ReplicationTargetReadPreference --
CREATE TYPE target.target_read_preference AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.target.ReplicationTargetState --
CREATE TYPE target.target_state AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.target.Ds3TargetAccessControlReplication --
CREATE TYPE target.ds3_target_access_control_replication AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.target.Ds3Target --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='ds3_target') THEN
  CREATE TABLE target.ds3_target (
    access_control_replication target.ds3_target_access_control_replication NOT NULL,
    admin_auth_id    varchar                NOT NULL,
    admin_secret_key varchar                NOT NULL,
    data_path_end_point varchar             NOT NULL,
    data_path_https  boolean                NOT NULL,
    data_path_port   integer                NULL,
    data_path_proxy  varchar                NULL,
    data_path_verify_certificate boolean    NOT NULL,
    default_read_preference target.target_read_preference NOT NULL,
    id               uuid                   NOT NULL,
    name             varchar                NOT NULL,
    permit_going_out_of_sync    boolean     NOT NULL,
    quiesced         shared.quiesced        NOT NULL,
    replicated_user_default_data_policy varchar NULL,
    state            target.target_state    NOT NULL,

    PRIMARY KEY (id),
    UNIQUE (name)
  );
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType --
CREATE TYPE ds3.data_replication_rule_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='data_replication_rule') THEN
  CREATE TABLE ds3.data_replication_rule (
    data_policy_id   uuid                   NOT NULL   REFERENCES ds3.data_policy ON UPDATE CASCADE,
    ds3_target_data_policy varchar          NULL,
    ds3_target_id    uuid                   NOT NULL   REFERENCES target.ds3_target ON UPDATE CASCADE,
    id               uuid                   NOT NULL,
    state            ds3.data_persistence_rule_state NOT NULL,
    type             ds3.data_replication_rule_type NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (data_policy_id, ds3_target_id)
  );
CREATE INDEX ON ds3.data_replication_rule (data_policy_id);
CREATE INDEX ON ds3.data_replication_rule (ds3_target_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.target.BlobTarget --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='blob_target') THEN
  CREATE TABLE target.blob_target (
    blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    ds3_target_id    uuid                   NOT NULL   REFERENCES target.ds3_target ON UPDATE CASCADE,
    id               uuid                   NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (blob_id, ds3_target_id)
  );
CREATE INDEX ON target.blob_target (blob_id);
CREATE INDEX ON target.blob_target (ds3_target_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.target.Ds3ReplicationTargetReadPreference --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='ds3_target_read_preference') THEN
  CREATE TABLE target.ds3_target_read_preference (
    bucket_id        uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
    id               uuid                   NOT NULL,
    read_preference  target.target_read_preference NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.ds3_target ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (target_id, bucket_id)
  );
CREATE INDEX ON target.ds3_target_read_preference (bucket_id);
CREATE INDEX ON target.ds3_target_read_preference (target_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.target.Ds3TargetFailureType --
CREATE TYPE target.ds3_target_failure_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.target.Ds3TargetFailure --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='target' AND tablename='ds3_target_failure') THEN
  CREATE TABLE target.ds3_target_failure (
    date             timestamp without time zone NOT NULL,
    error_message    varchar                NULL,
    id               uuid                   NOT NULL,
    target_id        uuid                   NOT NULL   REFERENCES target.ds3_target ON UPDATE CASCADE ON DELETE CASCADE,
    type             target.ds3_target_failure_type NOT NULL,

    PRIMARY KEY (id)
  );
CREATE INDEX ON target.ds3_target_failure (target_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.notification.Ds3TargetFailureNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='ds3_target_failure_notification_registration') THEN
  CREATE TABLE notification.ds3_target_failure_notification_registration (
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
CREATE INDEX ON notification.ds3_target_failure_notification_registration (user_id);
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


-- com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob --
alter table ds3.degraded_blob add column replication_rule_id uuid REFERENCES ds3.data_replication_rule ON UPDATE CASCADE ON DELETE CASCADE;
alter table ds3.degraded_blob alter column persistence_rule_id DROP NOT NULL;
CREATE INDEX ON ds3.degraded_blob (replication_rule_id);
ALTER TABLE ds3.degraded_blob ADD CONSTRAINT degraded_blob_replication_rule_id_blob_id UNIQUE (replication_rule_id, blob_id);


-- com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend --
alter table ds3.data_path_backend add column instance_id uuid;
update ds3.data_path_backend set instance_id=id;
alter table ds3.data_path_backend alter column instance_id set not null;


-- com.spectralogic.s3.common.dao.domain.ds3.DataPolicy --
alter table ds3.data_policy add column always_replicate_deletes boolean;
update ds3.data_policy set always_replicate_deletes=true;
alter table ds3.data_policy alter column always_replicate_deletes set not null;


-- com.spectralogic.s3.common.dao.domain.ds3.JobChunk --
ALTER TABLE ds3.job_chunk ADD COLUMN pending_target_commit boolean NULL;
UPDATE ds3.job_chunk set pending_target_commit = false;
ALTER TABLE ds3.job_chunk alter column pending_target_commit set not null;

ALTER TABLE ds3.job_chunk ADD COLUMN read_from_ds3_target_id uuid NULL REFERENCES target.ds3_target ON UPDATE CASCADE ON DELETE SET NULL;
CREATE INDEX ON ds3.job_chunk (read_from_ds3_target_id);
