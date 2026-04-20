class IncreaseMaxNumberOfConcurrentJobs < ActiveRecord::Migration[4.2]
  def up
    execute("ALTER TABLE ds3.data_path_backend ADD COLUMN IF NOT EXISTS max_number_of_concurrent_jobs INTEGER;")
    execute("UPDATE ds3.data_path_backend SET max_number_of_concurrent_jobs = 40000;")
    execute("ALTER TABLE ds3.data_path_backend ALTER COLUMN max_number_of_concurrent_jobs SET NOT NULL;")
  end

  def down
    execute("ALTER TABLE ds3.data_path_backend DROP COLUMN max_number_of_concurrent_jobs")
  end
end