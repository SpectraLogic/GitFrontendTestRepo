require "spectra_support/cmd"

class ModifyBlobTapeIndex < ActiveRecord::Migration[4.2]
  def up
    execute('ALTER TABLE tape.blob_tape DROP CONSTRAINT IF EXISTS blob_tape_tape_id_order_index_key')
    execute('ALTER TABLE tape.blob_tape ADD UNIQUE (tape_id, order_index)')
    execute('ALTER TABLE tape.blob_tape DROP CONSTRAINT IF EXISTS blob_tape_order_index_tape_id_key')
  end

  def down
    execute('ALTER TABLE tape.blob_tape DROP CONSTRAINT IF EXISTS blob_tape_order_index_tape_id_key')
    execute('ALTER TABLE tape.blob_tape ADD UNIQUE (order_index, tape_id)')
    execute('ALTER TABLE tape.blob_tape DROP CONSTRAINT IF EXISTS blob_tape_tape_id_order_index_key')
  end
end