class NakedJob < ActiveRecord::Migration[4.2]
  def up
    execute("alter table ds3.job add column naked boolean;")
    execute("update ds3.job set naked=false;")
    execute("alter table ds3.job alter column naked set not null;")

    execute("alter table ds3.completed_job add column naked boolean;")
    execute("update ds3.completed_job set naked=false;")
    execute("alter table ds3.completed_job alter column naked set not null;")

    execute("alter table ds3.canceled_job add column naked boolean;")
    execute("update ds3.canceled_job set naked=false;")
    execute("alter table ds3.canceled_job alter column naked set not null;")
  end
    
  def down
    execute("alter table ds3.job drop column naked;")
    execute("alter table ds3.completed_job drop column naked;")
    execute("alter table ds3.canceled_job drop column naked;")
  end
end

