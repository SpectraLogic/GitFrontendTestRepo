require "spectra_support/cmd"

class AddAbm3 < ActiveRecord::Migration[4.2]
  SQL_FILE = File.expand_path(__FILE__).sub(/rb$/, "sql")
  
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -f #{SQL_FILE} --single-transaction -v ON_ERROR_STOP=1")
  end
    
  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end

