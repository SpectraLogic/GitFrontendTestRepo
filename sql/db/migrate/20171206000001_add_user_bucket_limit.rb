require "spectra_support/cmd"

class AddUserBucketLimit < ActiveRecord::Migration[4.2]

  def up
    execute('ALTER TABLE ds3.user ADD COLUMN IF NOT EXISTS max_buckets INTEGER')
    execute('UPDATE ds3.user SET max_buckets = 10000')
    execute('ALTER TABLE ds3.user ALTER max_buckets SET NOT NULL')
  end

  def down
    execute('ALTER TABLE ds3.user DROP COLUMN IF EXISTS max_buckets')
  end
end
