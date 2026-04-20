class RestrictedS3Targets < ActiveRecord::Migration[4.2]
  def up
    execute("ALTER TABLE target.s3_target ADD restricted_access boolean;")
    execute("UPDATE target.s3_target SET restricted_access=false;")
    execute("ALTER TABLE target.s3_target ALTER COLUMN restricted_access SET not null;")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end
