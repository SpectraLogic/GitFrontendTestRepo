class AddCharacterizationVer < ActiveRecord::Migration[4.2]
  def up
    execute("ALTER TABLE tape.tape ADD COLUMN IF NOT EXISTS characterization_ver varchar NULL;")
    execute("ALTER TABLE tape.tape_drive ADD COLUMN IF NOT EXISTS characterization_ver varchar;")
  end

  def down
    execute("ALTER TABLE tape.tape DROP COLUMN IF EXISTS characterization_ver;")
    execute("ALTER TABLE tape.tape_drive DROP COLUMN IF EXISTS characterization_ver;")
  end
end