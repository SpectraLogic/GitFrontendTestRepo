class DropS3TargetUniqueIndex < ActiveRecord::Migration[4.2]
  def up
    execute("ALTER TABLE target.s3_target DROP CONSTRAINT s3_target_data_path_end_point_access_key;")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end
