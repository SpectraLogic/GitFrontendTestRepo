require "spectra_support/cmd"

class DropTableS3Folder < ActiveRecord::Migration[4.2]
  def up
    execute('DROP TABLE ds3.s3_folder')
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end