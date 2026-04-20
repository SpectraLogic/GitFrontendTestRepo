class ConfigureKeyValueUniqueIndex < ActiveRecord::Migration[4.2]
  def up
    execute("ALTER TABLE framework.key_value ADD UNIQUE (key);")
  end
    
  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end

