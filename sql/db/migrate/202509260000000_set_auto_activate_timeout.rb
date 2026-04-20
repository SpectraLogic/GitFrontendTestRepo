class SetAutoActivateTimeout < ActiveRecord::Migration[4.2]
  def up
    execute("UPDATE ds3.data_path_backend SET auto_activate_timeout_in_mins = NULL;")
  end

  def down
      raise(ActiveRecord::IrreversibleMigration)
    end
end