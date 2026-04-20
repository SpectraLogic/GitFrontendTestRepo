class AddPoolSafetyDisabled < ActiveRecord::Migration[4.2]
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
	execute("ALTER TABLE ds3.data_path_backend ADD COLUMN IF NOT EXISTS pool_safety_enabled boolean ")
	execute("UPDATE ds3.data_path_backend SET pool_safety_enabled=true")
	execute("ALTER TABLE ds3.data_path_backend ALTER COLUMN pool_safety_enabled SET NOT NULL")
  end

  def down
    execute("ALTER TABLE ds3.data_path_backend DROP COLUMN pool_safety_enabled")
  end
end
