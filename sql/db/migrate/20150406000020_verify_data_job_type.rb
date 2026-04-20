class VerifyDataJobType < ActiveRecord::Migration[4.2]
  def up
    execute("alter table ds3.bucket add column default_verify_job_priority ds3.blob_store_task_priority;")
    execute("update ds3.bucket set default_verify_job_priority='LOW';")
    execute("alter table ds3.bucket alter column default_verify_job_priority set not null;")
  end

  def down
    execute("alter table ds3.bucket drop column default_verify_job_priority;")
  end
end

