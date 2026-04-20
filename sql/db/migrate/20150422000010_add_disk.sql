-- Auto-generated
--    By: com.spectralogic.util.db.codegen.SqlCodeGenerator
--    On: Mon Dec 08 20:59:51 GMT 2014


-- SCHEMA shared --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = 'shared') THEN
  CREATE SCHEMA shared;
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.shared.Quiesced --
CREATE TYPE shared.quiesced AS ENUM ();


-- Fix TapePartition to use the correct enum type --
ALTER TYPE shared.quiesced add value if not exists 'YES';
COMMIT;
ALTER TABLE tape.tape_partition DROP COLUMN quiesced;
ALTER TABLE tape.tape_partition ADD COLUMN quiesced shared.quiesced;
UPDATE tape.tape_partition SET quiesced='YES';
ALTER TABLE tape.tape_partition ALTER COLUMN quiesced set not null;


-- SCHEMA pool --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM information_schema.schemata WHERE schema_name = 'pool') THEN
  CREATE SCHEMA pool;
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.pool.PoolHealth --
CREATE TYPE pool.pool_health AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.pool.PoolState --
CREATE TYPE pool.pool_state AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.pool.PoolType --
CREATE TYPE pool.pool_type AS ENUM ();

    
-- com.spectralogic.s3.common.dao.domain.pool.Pool --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='pool' AND tablename='pool') THEN
  CREATE TABLE pool.pool (
    available_capacity bigint                NOT NULL,
    guid              varchar                NOT NULL,
    health            pool.pool_health       NOT NULL,
    id                uuid                   NOT NULL,
    last_accessed     timestamp without time zone NULL,
    last_modified     timestamp without time zone NULL,
    last_verified     timestamp without time zone NULL,
    mountpoint        varchar                NOT NULL,
    name              varchar                NOT NULL,
    powered_on        boolean                NOT NULL,
    quiesced          shared.quiesced        NOT NULL,
    state             pool.pool_state        NOT NULL,
    type              pool.pool_type         NOT NULL,
    used_capacity     bigint                 NOT NULL,
    reserved_capacity bigint                 NOT NULL,

    PRIMARY KEY (id),
    UNIQUE (guid),
    UNIQUE (mountpoint),
    UNIQUE (name)
  );
END IF;
END
$$;
  


-- com.spectralogic.s3.common.dao.domain.pool.BlobDisk --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='pool' AND tablename='blob_pool') THEN
  CREATE TABLE pool.blob_pool (
    blob_id           uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    bucket_id         uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
    date_written      timestamp without time zone NOT NULL,
    id                uuid                   NOT NULL,
    last_accessed     timestamp without time zone NOT NULL,
    pool_id           uuid                   NOT NULL   REFERENCES pool.pool ON UPDATE CASCADE,
  
    PRIMARY KEY (id),
    UNIQUE (blob_id, pool_id)
  );
CREATE INDEX ON pool.blob_pool (blob_id);
CREATE INDEX ON pool.blob_pool (pool_id);
CREATE INDEX ON pool.blob_pool (bucket_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.pool.PoolFailureType --
CREATE TYPE pool.pool_failure_type AS ENUM ();


-- com.spectralogic.s3.common.dao.domain.pool.PoolFailure --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='pool' AND tablename='pool_failure') THEN
  CREATE TABLE pool.pool_failure (
    date              timestamp without time zone NOT NULL,
    error_message     varchar                NULL,
    id                uuid                   NOT NULL,
    pool_id           uuid                   NOT NULL   REFERENCES pool.pool ON UPDATE CASCADE ON DELETE CASCADE,
    type              pool.pool_failure_type NOT NULL,

    PRIMARY KEY (id)
  );
CREATE INDEX ON pool.pool_failure (pool_id);
END IF;
END
$$;


-- com.spectralogic.s3.common.dao.domain.notification.PoolFailureNotificationRegistration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='pool_failure_notification_registration') THEN
  CREATE TABLE notification.pool_failure_notification_registration (
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
CREATE INDEX ON notification.pool_failure_notification_registration (user_id);
END IF;
END
$$;

