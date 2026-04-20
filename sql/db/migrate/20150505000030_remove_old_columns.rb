class RemoveOldColumns < ActiveRecord::Migration[4.2]
  def up
    execute("DROP TABLE ds3.bucket_property")
    execute("ALTER TABLE ds3.bucket DROP COLUMN default_checksum, DROP COLUMN default_get_job_priority, DROP COLUMN default_put_job_priority, DROP COLUMN default_verify_job_priority, DROP COLUMN default_write_optimization, DROP COLUMN max_client_checksum, DROP COLUMN min_client_checksum, DROP COLUMN object_spanning")

    # All buckets should have a data policy assigned in the previous migration
    execute("ALTER TABLE ds3.bucket ALTER COLUMN data_policy_id SET NOT NULL")

    execute("ALTER TABLE ds3.canceled_job DROP COLUMN write_optimization")
    execute("ALTER TABLE ds3.completed_job DROP COLUMN write_optimization")
    execute("ALTER TABLE ds3.job DROP COLUMN write_optimization")

    execute("ALTER TABLE tape.tape DROP COLUMN assigned_to_bucket")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)    
  end

end
