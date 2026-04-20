require "spectra_support/cmd"

class AddVersioning < ActiveRecord::Migration[4.2]
  def up
    execute('DROP INDEX IF EXISTS ds3.ds3_s3_object__version')
    execute('ALTER TABLE ds3.s3_object DROP COLUMN IF EXISTS version')
    execute('CREATE INDEX IF NOT EXISTS ds3_s3_object__creation_date on ds3.s3_object (creation_date)')
    execute('ALTER TABLE ds3.data_policy ADD COLUMN IF NOT EXISTS max_versions_to_keep INTEGER')
    execute('UPDATE ds3.data_policy SET max_versions_to_keep=2147483647')
    execute('ALTER TABLE ds3.data_policy ALTER COLUMN max_versions_to_keep SET NOT NULL')
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end