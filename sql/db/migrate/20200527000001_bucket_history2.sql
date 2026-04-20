-- Table: notification.s3_objects_changed_latest_notification_registration --
DO $$
BEGIN
IF EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='bucket_history_event') THEN
  ALTER TABLE notification.bucket_history_event ADD object_creation_date timestamp without time zone;
END IF;
END
$$;
