class RemoveTrailingSlashFromFolderEntries < ActiveRecord::Migration[4.2]
  def up
    execute("UPDATE ds3.s3_folder SET name = trim(trailing '/' from name)")
  end

  def down
    execute("UPDATE ds3.s3_folder SET name = name || '/'")
  end
end

