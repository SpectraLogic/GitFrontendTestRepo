-- Auto-generated
--    By: com.spectralogic.util.db.codegen.SqlCodeGenerator
--    On: Fri Oct 17 20:22:06 GMT 2014

-- SCHEMA ds3 --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = 'ds3') THEN
  CREATE SCHEMA ds3;
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.User --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='user') THEN
  CREATE TABLE ds3.user (
    auth_id           varchar                NOT NULL,
    id                uuid                   NOT NULL,
    name              varchar                NOT NULL,
    secret_key        varchar                NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (auth_id), 
    UNIQUE (name)
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



-- com.spectralogic.util.security.ChecksumType --
CREATE TYPE security.checksum_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority --
CREATE TYPE ds3.blob_store_task_priority AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.WriteOptimization --
CREATE TYPE ds3.write_optimization AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.S3ObjectSpanningPolicy --
CREATE TYPE ds3.s3_object_spanning_policy AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.Bucket --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='bucket') THEN
  CREATE TABLE ds3.bucket (
    creation_date     timestamp without time zone NOT NULL,
    default_checksum  security.checksum_type NOT NULL,
    default_get_job_priority ds3.blob_store_task_priority NOT NULL,
    default_put_job_priority ds3.blob_store_task_priority NOT NULL,
    default_write_optimization ds3.write_optimization NOT NULL,
    id                uuid                   NOT NULL,
    max_client_checksum security.checksum_type NULL,
    min_client_checksum security.checksum_type NULL,
    name              varchar                NOT NULL,
    object_spanning   ds3.s3_object_spanning_policy NOT NULL,
    user_id           uuid                   NOT NULL   REFERENCES ds3.user ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (name)
  );
CREATE INDEX ds3_bucket__user_id on ds3.bucket (user_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.Group --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='group') THEN
  CREATE TABLE ds3.group (
    built_in          boolean                NOT NULL,
    id                uuid                   NOT NULL,
    name              varchar                NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (name)
  );
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.S3ObjectType --
CREATE TYPE ds3.s3_object_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.S3Object --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='s3_object') THEN
  CREATE TABLE ds3.s3_object (
    bucket_id         uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE,
    creation_date     timestamp without time zone NULL,
    id                uuid                   NOT NULL,
    name              varchar                NOT NULL,
    type              ds3.s3_object_type     NOT NULL,
    version           bigint                 NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (bucket_id, name)
  );
CREATE INDEX ds3_s3_object__bucket_id on ds3.s3_object (bucket_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.AclPermission --
CREATE TYPE ds3.acl_permission AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.Acl --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='acl') THEN
  CREATE TABLE ds3.acl (
    bucket_id         uuid                   NULL       REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
    group_id          uuid                   NULL       REFERENCES ds3.group ON UPDATE CASCADE ON DELETE CASCADE,
    id                uuid                   NOT NULL,
    object_id         uuid                   NULL       REFERENCES ds3.s3_object ON UPDATE CASCADE ON DELETE CASCADE,
    permission        ds3.acl_permission     NOT NULL,
    user_id           uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id)
  );
CREATE INDEX ds3_acl__bucket_id on ds3.acl (bucket_id);
CREATE INDEX ds3_acl__group_id on ds3.acl (group_id);
CREATE INDEX ds3_acl__object_id on ds3.acl (object_id);
CREATE INDEX ds3_acl__user_id on ds3.acl (user_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.Blob --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='blob') THEN
  CREATE TABLE ds3.blob (
    byte_offset       bigint                 NOT NULL,
    checksum          varchar                NULL,
    checksum_type     security.checksum_type NULL,
    id                uuid                   NOT NULL,
    length            bigint                 NOT NULL,
    multi_part_upload_parent_blob_id uuid    NULL       REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    object_id         uuid                   NOT NULL   REFERENCES ds3.s3_object ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (byte_offset, object_id)
  );
CREATE INDEX ds3_blob__multi_part_upload_parent_blob_id on ds3.blob (multi_part_upload_parent_blob_id);
CREATE INDEX ds3_blob__object_id on ds3.blob (object_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.BucketProperty --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='bucket_property') THEN
  CREATE TABLE ds3.bucket_property (
    bucket_id         uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
    id                uuid                   NOT NULL,
    key               varchar                NOT NULL,
    value             varchar                NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (bucket_id, key)
  );
CREATE INDEX ds3_bucket_property__bucket_id on ds3.bucket_property (bucket_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.GroupMember --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='group_member') THEN
  CREATE TABLE ds3.group_member (
    group_id          uuid                   NOT NULL   REFERENCES ds3.group ON UPDATE CASCADE ON DELETE CASCADE,
    id                uuid                   NOT NULL,
    member_group_id   uuid                   NULL       REFERENCES ds3.group ON UPDATE CASCADE ON DELETE CASCADE,
    member_user_id    uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (group_id, member_user_id), 
    UNIQUE (group_id, member_group_id)
  );
CREATE INDEX ds3_group_member__group_id on ds3.group_member (group_id);
CREATE INDEX ds3_group_member__member_group_id on ds3.group_member (member_group_id);
CREATE INDEX ds3_group_member__member_user_id on ds3.group_member (member_user_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.JobChunkClientProcessingOrderGuarantee --
CREATE TYPE ds3.job_chunk_client_processing_order_guarantee AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.JobRequestType --
CREATE TYPE ds3.job_request_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.Job --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='job') THEN
  CREATE TABLE ds3.job (
    bucket_id         uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
    cached_size_in_bytes bigint              NOT NULL,
    chunk_client_processing_order_guarantee ds3.job_chunk_client_processing_order_guarantee NOT NULL,
    completed_size_in_bytes bigint           NOT NULL,
    created_at        timestamp without time zone NOT NULL,
    id                uuid                   NOT NULL,
    original_size_in_bytes bigint            NOT NULL,
    priority          ds3.blob_store_task_priority NOT NULL,
    request_type      ds3.job_request_type   NOT NULL,
    user_id           uuid                   NOT NULL   REFERENCES ds3.user ON UPDATE CASCADE,
    write_optimization ds3.write_optimization NOT NULL,

    PRIMARY KEY (id)
  );
CREATE INDEX ds3_job__bucket_id on ds3.job (bucket_id);
CREATE INDEX ds3_job__user_id on ds3.job (user_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.Node --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='node') THEN
  CREATE TABLE ds3.node (
    data_path_ip_address varchar             NOT NULL,
    id                uuid                   NOT NULL,
    last_heartbeat    timestamp without time zone NOT NULL,
    name              varchar                NOT NULL,
    serial_number     varchar                NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (serial_number), 
    UNIQUE (name)
  );
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.JobChunkBlobStoreState --
CREATE TYPE ds3.job_chunk_blob_store_state AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.ds3.JobChunk --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='job_chunk') THEN
  CREATE TABLE ds3.job_chunk (
    blob_store_state  ds3.job_chunk_blob_store_state NOT NULL,
    chunk_number      integer                NOT NULL,
    id                uuid                   NOT NULL,
    job_id            uuid                   NOT NULL   REFERENCES ds3.job ON UPDATE CASCADE ON DELETE CASCADE,
    node_id           uuid                   NULL       REFERENCES ds3.node ON UPDATE CASCADE ON DELETE SET NULL,

    PRIMARY KEY (id), 
    UNIQUE (chunk_number, job_id)
  );
CREATE INDEX ds3_job_chunk__blob_store_state on ds3.job_chunk (blob_store_state);
CREATE INDEX ds3_job_chunk__job_id on ds3.job_chunk (job_id);
CREATE INDEX ds3_job_chunk__node_id on ds3.job_chunk (node_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.JobEntry --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='job_entry') THEN
  CREATE TABLE ds3.job_entry (
    blob_id           uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    chunk_id          uuid                   NOT NULL   REFERENCES ds3.job_chunk ON UPDATE CASCADE ON DELETE CASCADE,
    id                uuid                   NOT NULL,
    job_id            uuid                   NOT NULL   REFERENCES ds3.job ON UPDATE CASCADE ON DELETE CASCADE,
    order_index       integer                NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (job_id, blob_id), 
    UNIQUE (order_index, chunk_id)
  );
CREATE INDEX ds3_job_entry__blob_id on ds3.job_entry (blob_id);
CREATE INDEX ds3_job_entry__chunk_id on ds3.job_entry (chunk_id);
CREATE INDEX ds3_job_entry__job_id on ds3.job_entry (job_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.S3Folder --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='s3_folder') THEN
  CREATE TABLE ds3.s3_folder (
    bucket_id         uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
    id                uuid                   NOT NULL,
    name              varchar                NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (bucket_id, name)
  );
CREATE INDEX ds3_s3_folder__bucket_id on ds3.s3_folder (bucket_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='ds3' AND tablename='s3_object_property') THEN
  CREATE TABLE ds3.s3_object_property (
    id                uuid                   NOT NULL,
    key               varchar                NOT NULL,
    object_id         uuid                   NOT NULL   REFERENCES ds3.s3_object ON UPDATE CASCADE ON DELETE CASCADE,
    value             varchar                NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (object_id, key)
  );
CREATE INDEX ds3_s3_object_property__object_id on ds3.s3_object_property (object_id);
END IF;
END
$$;



-- SCHEMA notification --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = 'notification') THEN
  CREATE SCHEMA notification;
END IF;
END
$$;



-- SCHEMA http --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = 'http') THEN
  CREATE SCHEMA http;
END IF;
END
$$;



-- com.spectralogic.util.http.HttpResponseFormatType --
CREATE TYPE http.http_response_format_type AS ENUM ();


-- SCHEMA lang --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = 'lang') THEN
  CREATE SCHEMA lang;
END IF;
END
$$;



-- com.spectralogic.util.lang.NamingConventionType --
CREATE TYPE lang.naming_convention_type AS ENUM ();


-- com.spectralogic.util.http.RequestType --
CREATE TYPE http.request_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.notification.GenericDaoNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='generic_dao_notification_registration') THEN
  CREATE TABLE notification.generic_dao_notification_registration (
    creation_date     timestamp without time zone NOT NULL,
    dao_type          varchar                NOT NULL,
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
CREATE INDEX notification_generic_dao_notification_registration__user_id on notification.generic_dao_notification_registration (user_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.notification.JobCompletedNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='job_completed_notification_registration') THEN
  CREATE TABLE notification.job_completed_notification_registration (
    creation_date     timestamp without time zone NOT NULL,
    format            http.http_response_format_type NOT NULL,
    id                uuid                   NOT NULL,
    job_id            uuid                   NULL       REFERENCES ds3.job ON UPDATE CASCADE ON DELETE CASCADE,
    last_http_response_code integer          NULL,
    last_notification timestamp without time zone NULL,
    naming_convention lang.naming_convention_type NOT NULL,
    notification_end_point varchar           NOT NULL,
    notification_http_method http.request_type NOT NULL,
    number_of_failures_since_last_success integer NOT NULL,
    user_id           uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id)
  );
CREATE INDEX notification_job_completed_notification_registration__job_id on notification.job_completed_notification_registration (job_id);
CREATE INDEX notification_job_completed_notification_registration__user_id on notification.job_completed_notification_registration (user_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.notification.JobCreatedNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='job_created_notification_registration') THEN
  CREATE TABLE notification.job_created_notification_registration (
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
CREATE INDEX notification_job_created_notification_registration__user_id on notification.job_created_notification_registration (user_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.notification.S3ObjectCachedNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='s3_object_cached_notification_registration') THEN
  CREATE TABLE notification.s3_object_cached_notification_registration (
    creation_date     timestamp without time zone NOT NULL,
    format            http.http_response_format_type NOT NULL,
    id                uuid                   NOT NULL,
    job_id            uuid                   NULL       REFERENCES ds3.job ON UPDATE CASCADE ON DELETE CASCADE,
    last_http_response_code integer          NULL,
    last_notification timestamp without time zone NULL,
    naming_convention lang.naming_convention_type NOT NULL,
    notification_end_point varchar           NOT NULL,
    notification_http_method http.request_type NOT NULL,
    number_of_failures_since_last_success integer NOT NULL,
    user_id           uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id)
  );
CREATE INDEX notification_s3_object_cached_notification_registration__job_id on notification.s3_object_cached_notification_registration (job_id);
CREATE INDEX notifixtion_s3_object_cached_notification_registration__user_id on notification.s3_object_cached_notification_registration (user_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.notification.S3ObjectLostNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='s3_object_lost_notification_registration') THEN
  CREATE TABLE notification.s3_object_lost_notification_registration (
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
CREATE INDEX notification_s3_object_lost_notification_registration__user_id on notification.s3_object_lost_notification_registration (user_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.notification.S3ObjectPersistedNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='s3_object_persisted_notification_registration') THEN
  CREATE TABLE notification.s3_object_persisted_notification_registration (
    creation_date     timestamp without time zone NOT NULL,
    format            http.http_response_format_type NOT NULL,
    id                uuid                   NOT NULL,
    job_id            uuid                   NULL       REFERENCES ds3.job ON UPDATE CASCADE ON DELETE CASCADE,
    last_http_response_code integer          NULL,
    last_notification timestamp without time zone NULL,
    naming_convention lang.naming_convention_type NOT NULL,
    notification_end_point varchar           NOT NULL,
    notification_http_method http.request_type NOT NULL,
    number_of_failures_since_last_success integer NOT NULL,
    user_id           uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,

    PRIMARY KEY (id)
  );
CREATE INDEX notifixtion_s3_object_persistednotifixtion_registration__job_id on notification.s3_object_persisted_notification_registration (job_id);
CREATE INDEX notifixtion_s3_object_persistednxifixtion_registration__user_id on notification.s3_object_persisted_notification_registration (user_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.notification.TapeFailureNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='tape_failure_notification_registration') THEN
  CREATE TABLE notification.tape_failure_notification_registration (
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
CREATE INDEX notification_tape_failure_notification_registration__user_id on notification.tape_failure_notification_registration (user_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.notification.TapePartitionFailureNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='tape_partition_failure_notification_registration') THEN
  CREATE TABLE notification.tape_partition_failure_notification_registration (
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
CREATE INDEX notifixtion_tape_partition_failurexixtion_registration__user_id on notification.tape_partition_failure_notification_registration (user_id);
END IF;
END
$$;



-- SCHEMA planner --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = 'planner') THEN
  CREATE SCHEMA planner;
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='planner' AND tablename='cache_filesystem') THEN
  CREATE TABLE planner.cache_filesystem (
    id                uuid                   NOT NULL,
    max_capacity_in_bytes bigint             NULL,
    max_percent_utilization_of_filesystem double precision NULL,
    node_id           uuid                   NOT NULL   REFERENCES ds3.node ON UPDATE CASCADE ON DELETE CASCADE,
    path              varchar                NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (node_id)
  );
CREATE INDEX planner_cache_filesystem__node_id on planner.cache_filesystem (node_id);
END IF;
END
$$;



-- SCHEMA tape --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = 'tape') THEN
  CREATE SCHEMA tape;
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.tape.TapeLibrary --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='tape' AND tablename='tape_library') THEN
  CREATE TABLE tape.tape_library (
    id                uuid                   NOT NULL,
    management_url    varchar                NOT NULL,
    name              varchar                NOT NULL,
    serial_number     varchar                NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (serial_number), 
    UNIQUE (name)
  );
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.tape.TapePartitionQuiesced --
CREATE TYPE tape.tape_partition_quiesced AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.tape.TapePartitionState --
CREATE TYPE tape.tape_partition_state AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.tape.TapePartition --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='tape' AND tablename='tape_partition') THEN
  CREATE TABLE tape.tape_partition (
    error_message     varchar                NULL,
    id                uuid                   NOT NULL,
    library_id        uuid                   NOT NULL   REFERENCES tape.tape_library ON UPDATE CASCADE,
    name              varchar                NOT NULL,
    quiesced          tape.tape_partition_quiesced NOT NULL,
    serial_number     varchar                NOT NULL,
    state             tape.tape_partition_state NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (serial_number), 
    UNIQUE (name)
  );
CREATE INDEX tape_tape_partition__library_id on tape.tape_partition (library_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.tape.TapeState --
CREATE TYPE tape.tape_state AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.tape.TapeType --
CREATE TYPE tape.tape_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.tape.Tape --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='tape' AND tablename='tape') THEN
  CREATE TABLE tape.tape (
    available_raw_capacity bigint            NULL,
    bar_code          varchar                NOT NULL,
    bucket_id         uuid                   NULL       REFERENCES ds3.bucket ON UPDATE CASCADE,
    description_for_identification varchar   NULL,
    full_of_data      boolean                NOT NULL,
    id                uuid                   NOT NULL,
    last_accessed     timestamp without time zone NULL,
    last_checkpoint   varchar                NULL,
    last_modified     timestamp without time zone NULL,
    last_verified     timestamp without time zone NULL,
    partition_id      uuid                   NULL       REFERENCES tape.tape_partition ON UPDATE CASCADE ON DELETE CASCADE,
    previous_state    tape.tape_state        NULL,
    serial_number     varchar                NULL,
    state             tape.tape_state        NOT NULL,
    total_raw_capacity bigint                NULL,
    type              tape.tape_type         NOT NULL,
    write_protected   boolean                NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (bar_code, partition_id), 
    UNIQUE (serial_number)
  );
CREATE INDEX tape_tape__bucket_id on tape.tape (bucket_id);
CREATE INDEX tape_tape__partition_id on tape.tape (partition_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.tape.BlobTape --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='tape' AND tablename='blob_tape') THEN
  CREATE TABLE tape.blob_tape (
    blob_id           uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    id                uuid                   NOT NULL,
    order_index       integer                NOT NULL,
    tape_id           uuid                   NOT NULL   REFERENCES tape.tape ON UPDATE CASCADE,

    PRIMARY KEY (id), 
    UNIQUE (blob_id, tape_id), 
    UNIQUE (order_index, tape_id)
  );
CREATE INDEX tape_blob_tape__blob_id on tape.blob_tape (blob_id);
CREATE INDEX tape_blob_tape__tape_id on tape.blob_tape (tape_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.tape.TapeDriveState --
CREATE TYPE tape.tape_drive_state AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.tape.TapeDriveType --
CREATE TYPE tape.tape_drive_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.tape.TapeDrive --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='tape' AND tablename='tape_drive') THEN
  CREATE TABLE tape.tape_drive (
    error_message     varchar                NULL,
    id                uuid                   NOT NULL,
    partition_id      uuid                   NOT NULL   REFERENCES tape.tape_partition ON UPDATE CASCADE ON DELETE CASCADE,
    serial_number     varchar                NOT NULL,
    state             tape.tape_drive_state  NOT NULL,
    tape_id           uuid                   NULL       REFERENCES tape.tape ON UPDATE CASCADE,
    type              tape.tape_drive_type   NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (serial_number)
  );
CREATE INDEX tape_tape_drive__partition_id on tape.tape_drive (partition_id);
CREATE INDEX tape_tape_drive__tape_id on tape.tape_drive (tape_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.tape.TapeFailureType --
CREATE TYPE tape.tape_failure_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.tape.TapeFailure --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='tape' AND tablename='tape_failure') THEN
  CREATE TABLE tape.tape_failure (
    date              timestamp without time zone NOT NULL,
    error_message     varchar                NULL,
    id                uuid                   NOT NULL,
    tape_drive_id     uuid                   NOT NULL   REFERENCES tape.tape_drive ON UPDATE CASCADE ON DELETE CASCADE,
    tape_id           uuid                   NOT NULL   REFERENCES tape.tape ON UPDATE CASCADE ON DELETE CASCADE,
    type              tape.tape_failure_type NOT NULL,

    PRIMARY KEY (id)
  );
CREATE INDEX tape_tape_failure__tape_drive_id on tape.tape_failure (tape_drive_id);
CREATE INDEX tape_tape_failure__tape_id on tape.tape_failure (tape_id);
END IF;
END
$$;



-- com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType --
CREATE TYPE tape.tape_partition_failure_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='tape' AND tablename='tape_partition_failure') THEN
  CREATE TABLE tape.tape_partition_failure (
    date              timestamp without time zone NOT NULL,
    error_message     varchar                NULL,
    id                uuid                   NOT NULL,
    partition_id      uuid                   NOT NULL   REFERENCES tape.tape_partition ON UPDATE CASCADE ON DELETE CASCADE,
    type              tape.tape_partition_failure_type NOT NULL,

    PRIMARY KEY (id)
  );
CREATE INDEX tape_tape_partition_failure__partition_id on tape.tape_partition_failure (partition_id);
END IF;
END
$$;



-- SCHEMA framework --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = 'framework') THEN
  CREATE SCHEMA framework;
END IF;
END
$$;



-- com.spectralogic.util.db.domain.KeyValue --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='framework' AND tablename='key_value') THEN
  CREATE TABLE framework.key_value (
    boolean_value     boolean                NULL,
    double_value      double precision       NULL,
    id                uuid                   NOT NULL,
    key               varchar                NOT NULL,
    long_value        bigint                 NULL,
    string_value      varchar                NULL,

    PRIMARY KEY (id)
  );
END IF;
END
$$;



-- com.spectralogic.util.db.domain.Mutex --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='framework' AND tablename='mutex') THEN
  CREATE TABLE framework.mutex (
    application_identifier uuid              NOT NULL,
    date_created      timestamp without time zone NOT NULL,
    id                uuid                   NOT NULL,
    last_heartbeat    timestamp without time zone NOT NULL,
    name              varchar                NOT NULL,

    PRIMARY KEY (id), 
    UNIQUE (name)
  );
END IF;
END
$$;



-- Grant permissions to Administrator --
GRANT USAGE ON SCHEMA http TO "Administrator";
GRANT ALL ON ALL SEQUENCES IN SCHEMA http TO "Administrator";
GRANT ALL ON ALL TABLES IN SCHEMA http TO "Administrator";
GRANT USAGE ON SCHEMA notification TO "Administrator";
GRANT ALL ON ALL SEQUENCES IN SCHEMA notification TO "Administrator";
GRANT ALL ON ALL TABLES IN SCHEMA notification TO "Administrator";
GRANT USAGE ON SCHEMA tape TO "Administrator";
GRANT ALL ON ALL SEQUENCES IN SCHEMA tape TO "Administrator";
GRANT ALL ON ALL TABLES IN SCHEMA tape TO "Administrator";
GRANT USAGE ON SCHEMA planner TO "Administrator";
GRANT ALL ON ALL SEQUENCES IN SCHEMA planner TO "Administrator";
GRANT ALL ON ALL TABLES IN SCHEMA planner TO "Administrator";
GRANT USAGE ON SCHEMA security TO "Administrator";
GRANT ALL ON ALL SEQUENCES IN SCHEMA security TO "Administrator";
GRANT ALL ON ALL TABLES IN SCHEMA security TO "Administrator";
GRANT USAGE ON SCHEMA framework TO "Administrator";
GRANT ALL ON ALL SEQUENCES IN SCHEMA framework TO "Administrator";
GRANT ALL ON ALL TABLES IN SCHEMA framework TO "Administrator";
GRANT USAGE ON SCHEMA lang TO "Administrator";
GRANT ALL ON ALL SEQUENCES IN SCHEMA lang TO "Administrator";
GRANT ALL ON ALL TABLES IN SCHEMA lang TO "Administrator";
GRANT USAGE ON SCHEMA ds3 TO "Administrator";
GRANT ALL ON ALL SEQUENCES IN SCHEMA ds3 TO "Administrator";
GRANT ALL ON ALL TABLES IN SCHEMA ds3 TO "Administrator";
