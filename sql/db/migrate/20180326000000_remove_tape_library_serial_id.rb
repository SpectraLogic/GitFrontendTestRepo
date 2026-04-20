require "spectra_support/cmd"

class RemoveTapeLibrarySerialId < ActiveRecord::Migration[4.2]
  def up
    execute('ALTER TABLE tape.tape_partition DROP COLUMN IF EXISTS serial_id')
  end

  def down
    execute('ALTER TABLE tape.tape_partition ADD COLUMN IF NOT EXISTS serial_id VARCHAR')
  end
end