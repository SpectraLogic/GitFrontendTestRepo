require "spectra_support/cmd"

class AddAllowNewJobRequests < ActiveRecord::Migration[4.2]
  def up
    execute('ALTER TABLE ds3.data_path_backend ADD COLUMN IF NOT EXISTS allow_new_job_requests BOOLEAN')
    execute('UPDATE ds3.data_path_backend SET allow_new_job_requests = true')
    execute('ALTER TABLE ds3.data_path_backend ALTER COLUMN allow_new_job_requests SET NOT NULL')
  end

  def down
    execute('ALTER TABLE ds3.data_path_backend DROP COLUMN IF EXISTS allow_new_job_requests')
  end
end
