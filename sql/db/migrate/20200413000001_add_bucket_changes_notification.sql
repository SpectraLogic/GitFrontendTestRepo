-- Table: notification.bucket_changes_notification_registration --
DO $$
BEGIN
IF NOT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname='notification' AND tablename='bucket_changes_notification_registration') THEN
CREATE TABLE notification.bucket_changes_notification_registration
(
  creation_date timestamp without time zone NOT NULL,
  format http.http_response_format_type NOT NULL,
  id uuid NOT NULL,
  last_failure     varchar                 NULL,
  last_http_response_code integer          NULL,
  last_notification timestamp without time zone NULL,
  naming_convention lang.naming_convention_type NOT NULL,
  notification_end_point varchar NOT NULL,
  notification_http_method http.request_type NOT NULL,
  number_of_failures_since_last_success integer NOT NULL,
  user_id           uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,
  bucket_id uuid                           NULL       REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
  last_sequence_number bigint              NULL,
  PRIMARY KEY (id)
  );
CREATE INDEX notification_bucket_changes_notif_reg__user_id on notification.bucket_changes_notification_registration (user_id);
CREATE INDEX IF NOT EXISTS notification_bucket_changes_notif_reg__creation_date
  ON notification.bucket_changes_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_bucket_changes_notif_reg__bucket_id
  ON notification.bucket_changes_notification_registration (bucket_id);
END IF;
END
$$;
