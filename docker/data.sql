
DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type t
                                         JOIN pg_enum e ON t.oid = e.enumtypid
                       WHERE t.typname = 'feature_key_type' AND e.enumlabel = 'AWS_S3_CLOUD_OUT') THEN
            ALTER TYPE ds3.feature_key_type ADD VALUE 'AWS_S3_CLOUD_OUT';
        END IF;
    END $$;

DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_type t
                              JOIN pg_enum e ON t.oid = e.enumtypid
            WHERE t.typname = 'feature_key_type' AND e.enumlabel = 'AWS_S3_CLOUD_OUT'
        ) THEN
            ALTER TYPE ds3.feature_key_type ADD VALUE 'AWS_S3_CLOUD_OUT';
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM pg_type t
                              JOIN pg_enum e ON t.oid = e.enumtypid
            WHERE t.typname = 'feature_key_type' AND e.enumlabel = 'MICROSOFT_AZURE_CLOUD_OUT'
        ) THEN
            ALTER TYPE ds3.feature_key_type ADD VALUE 'MICROSOFT_AZURE_CLOUD_OUT';
        END IF;
    END
$$;



DO $$
    DECLARE
        newid UUID := gen_random_uuid();
    BEGIN
        INSERT INTO  ds3.node (
            data_path_http_port,
            data_path_https_port,
            data_path_ip_address,
            id,
            last_heartbeat,
            name,
            serial_number
        ) VALUES (8080, 8443,'127.0.0.1', newid, now(), 'TEST_SERIAL', 'TEST_SERIAL');


        INSERT INTO planner.cache_filesystem (
            auto_reclaim_initiate_threshold,
            auto_reclaim_terminate_threshold,
            max_percent_utilization_of_filesystem,
            burst_threshold,
            cache_safety_enabled,
            max_capacity_in_bytes,
            needs_reconcile,
            path,
            id,
            node_id)
        VALUES (0.82, 0.72, 0.9,0.85,'0', 524288000, '1', '/tmp/cache', gen_random_uuid(), newid);

        INSERT INTO ds3.user (
            auth_id,
            id,
            max_buckets,
            name,
            secret_key
        )
        VALUES ('Administrator_authid', gen_random_uuid(), 1000, 'Administrator','mySecretKey');

        INSERT INTO ds3.feature_key (
            current_value,
            id,
            key,
            limit_value
        )
        VALUES
            (5, gen_random_uuid(), 'AWS_S3_CLOUD_OUT', 1152921504606846976),
            (5, gen_random_uuid(), 'MICROSOFT_AZURE_CLOUD_OUT', 1152921504606846976);END
$$;

