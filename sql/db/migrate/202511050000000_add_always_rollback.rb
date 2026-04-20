require "spectra_support/cmd"

class AddAlwaysRollback < ActiveRecord::Migration[4.2]
  def up
    execute('ALTER TABLE ds3.data_path_backend ADD COLUMN IF NOT EXISTS always_rollback BOOLEAN')
    execute('UPDATE ds3.data_path_backend SET always_rollback = true')
    execute('ALTER TABLE ds3.data_path_backend ALTER COLUMN always_rollback SET NOT NULL')
  end

  def down
    execute('ALTER TABLE ds3.data_path_backend DROP COLUMN IF EXISTS always_rollback')
  end
end

