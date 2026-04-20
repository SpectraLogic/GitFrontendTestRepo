require "spectra_support/cmd"

class DataLossRename < ActiveRecord::Migration[4.2]
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
    
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type ds3.system_failure_type add value if not exists 'SUSPECTED_DATA_LOSS_REQUIRES_USER_CONFIRMATION';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type ds3.system_failure_type add value if not exists 'CRITICAL_DATA_VERIFICATION_ERROR_REQUIRES_USER_CONFIRMATION';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"update ds3.system_failure set type='CRITICAL_DATA_VERIFICATION_ERROR_REQUIRES_USER_CONFIRMATION' where type='SUSPECTED_DATA_LOSS_REQUIRES_USER_CONFIRMATION';\"")
  end
    
  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end

