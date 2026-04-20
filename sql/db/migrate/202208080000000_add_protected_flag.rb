class AddProtectedFlag < ActiveRecord::Migration[4.2]
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]

	execute("ALTER TABLE ds3.bucket ADD COLUMN IF NOT EXISTS protected boolean")
	execute("UPDATE ds3.bucket SET protected=false")
	execute("ALTER TABLE ds3.bucket ALTER COLUMN protected SET NOT NULL")

	execute("ALTER TABLE ds3.job ADD COLUMN IF NOT EXISTS protected boolean")
    execute("UPDATE ds3.job SET protected=false")
    execute("ALTER TABLE ds3.job ALTER COLUMN protected SET NOT NULL")
  end

  def down
    execute("ALTER TABLE ds3.bucket DROP COLUMN protected")
    execute("ALTER TABLE ds3.job DROP COLUMN protected")
  end
end
