class AddIomCacheLimitationPercent < ActiveRecord::Migration[4.2]
  def up
    execute("ALTER TABLE ds3.data_path_backend ADD COLUMN IF NOT EXISTS iom_cache_limitation_percent double precision;")
    execute("UPDATE ds3.data_path_backend SET iom_cache_limitation_percent = 0.5;")
    execute("ALTER TABLE ds3.data_path_backend ALTER COLUMN iom_cache_limitation_percent SET NOT NULL;")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end
