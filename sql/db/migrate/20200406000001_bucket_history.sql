-- Table: notification.s3_objects_changed_latest_notification_registration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='bucket_history_event') THEN
CREATE TYPE notification.bucket_history_event_type AS ENUM ();

CREATE TABLE notification.bucket_history_event
(
  id uuid NOT NULL,
  sequence_number       bigserial                               NOT NULL,
  object_name           varchar                                 NOT NULL,
  bucket_id             uuid                                    NOT NULL       REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
  version_id            uuid                                    NOT NULL,
  type                  notification.bucket_history_event_type  NOT NULL,
  PRIMARY KEY (id)
  );
CREATE UNIQUE INDEX IF NOT EXISTS bucket_history_event__sequence_number
  ON notification.bucket_history_event (sequence_number);
CREATE INDEX IF NOT EXISTS bucket_history_event__bucket_id
  ON notification.bucket_history_event (bucket_id);
END IF;
END
$$;
