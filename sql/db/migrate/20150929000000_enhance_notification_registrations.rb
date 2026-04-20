class EnhanceNotificationRegistrations < ActiveRecord::Migration[4.2]
  def up
    execute("ALTER TABLE notification.generic_dao_notification_registration ADD COLUMN last_failure varchar;")
    execute("ALTER TABLE notification.s3_object_cached_notification_registration ADD COLUMN last_failure varchar;")
    execute("ALTER TABLE notification.tape_failure_notification_registration ADD COLUMN last_failure varchar;")
    execute("ALTER TABLE notification.job_completed_notification_registration ADD COLUMN last_failure varchar;")
    execute("ALTER TABLE notification.s3_object_lost_notification_registration ADD COLUMN last_failure varchar;")
    execute("ALTER TABLE notification.tape_partition_failure_notification_registration ADD COLUMN last_failure varchar;")
    execute("ALTER TABLE notification.job_created_notification_registration ADD COLUMN last_failure varchar;")
    execute("ALTER TABLE notification.s3_object_persisted_notification_registration ADD COLUMN last_failure varchar;")
    execute("ALTER TABLE notification.pool_failure_notification_registration ADD COLUMN last_failure varchar;")
    execute("ALTER TABLE notification.storage_domain_failure_notification_registration ADD COLUMN last_failure varchar;")
  end

  def down
    execute("ALTER TABLE notification.generic_dao_notification_registration DROP COLUMN last_failure;")  
    execute("ALTER TABLE notification.s3_object_cached_notification_registration DROP COLUMN last_failure;")  
    execute("ALTER TABLE notification.tape_failure_notification_registration DROP COLUMN last_failure;")  
    execute("ALTER TABLE notification.job_completed_notification_registration DROP COLUMN last_failure;")  
    execute("ALTER TABLE notification.s3_object_lost_notification_registration DROP COLUMN last_failure;")  
    execute("ALTER TABLE notification.tape_partition_failure_notification_registration DROP COLUMN last_failure;")  
    execute("ALTER TABLE notification.job_created_notification_registration DROP COLUMN last_failure;")  
    execute("ALTER TABLE notification.s3_object_persisted_notification_registration DROP COLUMN last_failure;")  
    execute("ALTER TABLE notification.pool_failure_notification_registration DROP COLUMN last_failure;")  
    execute("ALTER TABLE notification.storage_domain_failure_notification_registration DROP COLUMN last_failure;")  
  end
end
