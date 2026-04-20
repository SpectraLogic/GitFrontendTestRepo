require "spectra_support/cmd"

class RemoveFaultyTapeLibraryPartitionUniqueConstraints < ActiveRecord::Migration[4.2]
  def up
    execute('ALTER TABLE tape.tape_library DROP CONSTRAINT IF EXISTS tape_library_name_key')
    execute('CREATE INDEX IF NOT EXISTS tape_library_name_key ON tape.tape_library( name )')
    execute('ALTER TABLE tape.tape_partition DROP CONSTRAINT IF EXISTS tape_partition_name_key')
    execute('CREATE INDEX IF NOT EXISTS tape_partition_name_key ON tape.tape_partition( name )')
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end