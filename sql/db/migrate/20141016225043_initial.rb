require "spectra_support/cmd"

# Initial migration to setup the database.  This simply runs the corresponding
# .sql file that is checked into the same directly.  That .sql file is the
# "dao.sql" file that the frontend generated at the time of this writing.
# Using an external sql file was that easiest way to handle this initial
# migration.  We could have done something like insert its contents or the
# contents of the db/structure.sql schema file into this migration and ran
# that as well.
class Initial < ActiveRecord::Migration[4.2]
  SQL_FILE = File.expand_path(__FILE__).sub(/rb$/, "sql")

  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -f #{SQL_FILE} --single-transaction -v ON_ERROR_STOP=1")

    execute("SET search_path = public, pg_catalog;")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end
