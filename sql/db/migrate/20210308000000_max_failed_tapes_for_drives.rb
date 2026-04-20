require "spectra_support/cmd"

class MaxFailedTapesForDrives < ActiveRecord::Migration[4.2]
  def up
    execute("ALTER TABLE tape.tape_drive ADD COLUMN IF NOT EXISTS max_failed_tapes INTEGER;")
    execute("UPDATE tape.tape_drive SET max_failed_tapes = 3;")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end