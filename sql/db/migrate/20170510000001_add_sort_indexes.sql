-- ds3 schema --
CREATE INDEX IF NOT EXISTS ds3_s3_object__version
  ON ds3.s3_object (version);
CREATE INDEX IF NOT EXISTS ds3_canceled_job__date_canceled
  ON ds3.canceled_job (date_canceled);
CREATE INDEX IF NOT EXISTS ds3_canceled_job__created_at
  ON ds3.canceled_job (created_at);
CREATE INDEX IF NOT EXISTS ds3_canceled_job__name
  ON ds3.canceled_job (name);
CREATE INDEX IF NOT EXISTS ds3_completed_job__date_completed
  ON ds3.completed_job (date_completed);
CREATE INDEX IF NOT EXISTS ds3_completed_job__created_at
  ON ds3.completed_job (created_at);
CREATE INDEX IF NOT EXISTS ds3_completed_job__name
  ON ds3.completed_job (name);
CREATE INDEX IF NOT EXISTS ds3_job__created_at
  ON ds3.job (created_at);
CREATE INDEX IF NOT EXISTS ds3_job__name
  ON ds3.job (name);
CREATE INDEX IF NOT EXISTS ds3_storage_domain_failure__date
  ON ds3.storage_domain_failure (date);
CREATE INDEX IF NOT EXISTS ds3_storage_domain_member__state
  ON ds3.storage_domain_member (state);
CREATE INDEX IF NOT EXISTS ds3_storage_domain_member__write_preference
  ON ds3.storage_domain_member (write_preference);
CREATE INDEX IF NOT EXISTS ds3_system_failure__date
  ON ds3.system_failure (date);
-- notification schema --
CREATE INDEX IF NOT EXISTS notification_azure_target_failure_notification_registration__creation_date
  ON notification.azure_target_failure_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_ds3_target_failure_notification_registration__creation_date
  ON notification.ds3_target_failure_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_generic_dao_notification_registration__creation_date
  ON notification.generic_dao_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_job_completed_notification_registration__creation_date
  ON notification.job_completed_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_job_created_notification_registration__creation_date
  ON notification.job_created_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_job_creation_failed_notification_registration__creation_date
  ON notification.job_creation_failed_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_pool_failure_notification_registration__creation_date
  ON notification.pool_failure_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_s3_object_cached_notification_registration__creation_date
  ON notification.s3_object_cached_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_s3_object_lost_notification_registration__creation_date
  ON notification.s3_object_lost_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_s3_object_persisted_notification_registration__creation_date
  ON notification.s3_object_persisted_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_s3_target_failure_notification_registration__creation_date
  ON notification.s3_target_failure_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_storage_domain_failure_notification_registration__creation_date
  ON notification.storage_domain_failure_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_system_failure_notification_registration__creation_date
  ON notification.system_failure_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_tape_failure_notification_registration__creation_date
  ON notification.tape_failure_notification_registration (creation_date);
CREATE INDEX IF NOT EXISTS notification_tape_partition_failure_notification_registration__creation_date
  ON notification.tape_partition_failure_notification_registration (creation_date);
-- pool schema --
CREATE INDEX IF NOT EXISTS pool_pool_failure__date
  ON pool.pool_failure (date);
-- tape schema --
CREATE INDEX IF NOT EXISTS tape_tape__state
  ON tape.tape (state);
CREATE INDEX IF NOT EXISTS tape_tape__type
  ON tape.tape (type);
CREATE INDEX IF NOT EXISTS tape_tape_drive__state
  ON tape.tape_drive (state);
CREATE INDEX IF NOT EXISTS tape_tape_drive__type
  ON tape.tape_drive (type);
CREATE INDEX IF NOT EXISTS tape_tape_failure__date
  ON tape.tape_failure (date);
CREATE INDEX IF NOT EXISTS tape_tape_partition__state
  ON tape.tape_partition (state);
CREATE INDEX IF NOT EXISTS tape_tape_partition_failure__date
  ON tape.tape_partition_failure (date);
-- target schema --
CREATE INDEX IF NOT EXISTS target_azure_target__name
  ON target.azure_target (name);
CREATE INDEX IF NOT EXISTS target_azure_target_bucket_name__name
  ON target.azure_target_bucket_name (name);
CREATE INDEX IF NOT EXISTS target_azure_target_failure__date
  ON target.azure_target_failure (date);
CREATE INDEX IF NOT EXISTS target_ds3_target_failure__date
  ON target.ds3_target_failure (date);
CREATE INDEX IF NOT EXISTS target_s3_target__name
  ON target.s3_target (name);
CREATE INDEX IF NOT EXISTS target_s3_target_failure__date
  ON target.s3_target_failure (date);
CREATE INDEX IF NOT EXISTS target_s3_target_bucket_name__name
  ON target.s3_target_bucket_name (name);
