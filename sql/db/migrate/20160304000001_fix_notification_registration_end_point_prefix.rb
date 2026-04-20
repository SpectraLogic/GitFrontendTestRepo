# Update all GUI notification registration end point prefixes to handle
# the management REST API change that moved everything under the "/api/" path.
class FixNotificationRegistrationEndPointPrefix < ActiveRecord::Migration[4.2]
  # Only need to process the types of registrations the GUI registered for
  # at the time of the "/api/" change, which is only a small subset of all
  # the notification registrations the frontend supports.
  TABLES_GUI_REGISTERED_WITH = [
    "storage_domain_failure_notification_registration",
    "system_failure_notification_registration",
    "tape_partition_failure_notification_registration"
  ]

  OLD_PREFIX = "https://localhost/ds3/"
  NEW_PREFIX = "https://localhost/api/ds3/"

  def up
    migrate_registrations(OLD_PREFIX, NEW_PREFIX)
  end
  
  def down
    migrate_registrations(NEW_PREFIX, OLD_PREFIX)
  end

  private

  def migrate_registrations(old_prefix, new_prefix)
    TABLES_GUI_REGISTERED_WITH.each do |table|
      path = table.sub(/_registration/, "s")
      old_end_point = "#{old_prefix}#{path}"
      new_end_point = "#{new_prefix}#{path}"
      sql = "UPDATE notification.#{table} " \
            "SET notification_end_point='#{new_end_point}' " \
            "WHERE notification_end_point LIKE '#{old_end_point}';"
      execute(sql)
    end
  end
end
