class AddTapePartitionDriveIdleTimeoutInMinutes < ActiveRecord::Migration[4.2]
  def up
    execute("ALTER TABLE tape.tape_partition ADD COLUMN drive_idle_timeout_in_minutes INTEGER;")
    execute("UPDATE tape.tape_partition SET drive_idle_timeout_in_minutes = 15;")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end
