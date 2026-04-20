class DriveLastCleaned < ActiveRecord::Migration[4.2]
  def up
    execute("alter table tape.tape_drive add column last_cleaned timestamp without time zone;")
  end

  def down
    execute("alter table tape.tape_drive drop column last_cleaned;")
  end
end

