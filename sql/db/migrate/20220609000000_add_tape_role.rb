require "spectra_support/cmd"

class AddTapeRole < ActiveRecord::Migration[4.2]
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
    #NOTE: we use Cmd.run! instead of execute here in order to run the enum creation in a transaction so we can use it.
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"CREATE TYPE tape.tape_role AS ENUM ();\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"ALTER TYPE tape.tape_role add value if not exists 'NORMAL';\"")
    execute("ALTER TABLE tape.tape ADD COLUMN IF NOT EXISTS role tape.tape_role;")
    execute("UPDATE tape.tape SET role = 'NORMAL';")
    execute("ALTER TABLE tape.tape ALTER COLUMN role SET NOT NULL;")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end
