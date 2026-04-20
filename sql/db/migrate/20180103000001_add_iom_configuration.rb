require "spectra_support/cmd"

class AddIomConfiguration < ActiveRecord::Migration[4.2]
  def up
    execute('ALTER TABLE ds3.data_path_backend ADD COLUMN IF NOT EXISTS iom_enabled BOOLEAN')
    execute('UPDATE ds3.data_path_backend SET iom_enabled=true');
    execute('ALTER TABLE ds3.data_path_backend ALTER COLUMN iom_enabled SET NOT NULL');
    execute('ALTER TABLE tape.tape_partition ADD COLUMN IF NOT EXISTS auto_compaction_enabled BOOLEAN')
    execute('UPDATE tape.tape_partition SET auto_compaction_enabled=false');
    execute('ALTER TABLE tape.tape_partition ALTER COLUMN auto_compaction_enabled SET NOT NULL');
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end
