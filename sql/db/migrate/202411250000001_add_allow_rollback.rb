class AddAllowRollback < ActiveRecord::Migration[4.2]
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]

	execute("ALTER TABLE tape.tape ADD COLUMN IF NOT EXISTS allow_rollback boolean")
	execute("UPDATE tape.tape SET allow_rollback=false")
	execute("ALTER TABLE tape.tape ALTER COLUMN allow_rollback SET NOT NULL")
  end

  def down
    execute("ALTER TABLE tape.tape DROP COLUMN allow_rollback")
  end
end
