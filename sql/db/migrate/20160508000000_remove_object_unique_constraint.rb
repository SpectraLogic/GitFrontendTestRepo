class RemoveObjectUniqueConstraint < ActiveRecord::Migration[4.2]
  def up
    execute("ALTER TABLE ds3.s3_object DROP CONSTRAINT s3_object_bucket_id_name_version_idx;")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end

