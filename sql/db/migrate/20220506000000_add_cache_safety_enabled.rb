class AddCacheSafetyEnabled < ActiveRecord::Migration[4.2]
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
	execute("ALTER TABLE planner.cache_filesystem ADD COLUMN IF NOT EXISTS cache_safety_enabled boolean ")
	execute("UPDATE planner.cache_filesystem SET cache_safety_enabled=false")
	execute("ALTER TABLE planner.cache_filesystem ALTER COLUMN cache_safety_enabled SET NOT NULL")
  end

  def down
    execute("ALTER TABLE planner.cache_filesystem DROP COLUMN cache_safety_enabled")
  end
end
