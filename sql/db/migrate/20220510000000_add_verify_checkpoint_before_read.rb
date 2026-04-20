class AddVerifyCheckpointBeforeRead < ActiveRecord::Migration[4.2]
  def up
    execute("ALTER TABLE ds3.data_path_backend ADD COLUMN IF NOT EXISTS verify_checkpoint_before_read BOOLEAN;")
    execute("UPDATE ds3.data_path_backend SET verify_checkpoint_before_read = true;")
    execute("ALTER TABLE ds3.data_path_backend ALTER COLUMN verify_checkpoint_before_read SET NOT NULL;")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end
