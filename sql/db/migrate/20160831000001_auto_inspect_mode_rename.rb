require "spectra_support/cmd"

class AutoInspectModeRename < ActiveRecord::Migration[4.2]
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
    
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type ds3.auto_inspect_mode add value if not exists 'DEFAULT';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type ds3.auto_inspect_mode add value if not exists 'MINIMAL';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"update ds3.data_path_backend set auto_inspect='MINIMAL' where auto_inspect='DEFAULT';\"")
  end
    
  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end

