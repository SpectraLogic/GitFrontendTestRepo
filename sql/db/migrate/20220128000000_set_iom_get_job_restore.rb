require "spectra_support/cmd"

class SetIomGetJobRestore < ActiveRecord::Migration[4.2]
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"ALTER TYPE ds3.job_restore add value if not exists 'PERMANENT_ONLY';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"ALTER TYPE ds3.job_restore add value if not exists 'YES';\"")
    execute("UPDATE ds3.job SET restore = 'YES' WHERE EXISTS(SELECT * FROM ds3.data_migration WHERE get_job_id = job.id AND EXISTS(SELECT * FROM ds3.job WHERE id = data_migration.put_job_id AND restore = 'YES'));")
    execute("UPDATE ds3.job SET restore = 'PERMANENT_ONLY' WHERE EXISTS(SELECT * FROM ds3.data_migration WHERE get_job_id = job.id AND EXISTS(SELECT * FROM ds3.job WHERE id = data_migration.put_job_id AND restore = 'PERMANENT_ONLY'));")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end
