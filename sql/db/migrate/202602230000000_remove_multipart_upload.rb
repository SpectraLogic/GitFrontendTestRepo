class RemoveMultipartUpload < ActiveRecord::Migration[4.2]
  def up
    execute("DROP TABLE ds3.multi_part_upload_part;")
    execute("DROP TABLE ds3.multi_part_upload;")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end