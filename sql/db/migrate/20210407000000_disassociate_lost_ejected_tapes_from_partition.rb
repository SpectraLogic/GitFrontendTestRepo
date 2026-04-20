require "spectra_support/cmd"

class DisassociateLostEjectedTapesFromPartition < ActiveRecord::Migration[4.2]
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]

    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type tape.tape_state add value if not exists 'EJECTED';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type tape.tape_state add value if not exists 'LOST';\"")

    execute("UPDATE tape.tape SET partition_id = null WHERE state = 'EJECTED' OR state = 'LOST';")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end