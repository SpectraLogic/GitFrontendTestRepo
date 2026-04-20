class AddTapePartitionAutoQuiesceEnabled < ActiveRecord::Migration[4.2]
  def up
    execute("ALTER TABLE tape.tape_partition ADD COLUMN auto_quiesce_enabled BOOLEAN;")
    execute("UPDATE tape.tape_partition SET auto_quiesce_enabled = 'f';")
    execute("ALTER TABLE tape.tape_partition ALTER COLUMN auto_quiesce_enabled SET NOT NULL;")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end
