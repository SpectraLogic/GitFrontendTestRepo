require "spectra_support/cmd"

# This migration is being done via the command line because 'alter type'
# cannot be run inside a transaction.  Since all standard rails migrations run in
# transactions, this is a problem.
#
# Rails 4 provides support to run a migration outside of a transaction via the
# disable_ddl_transaction! function, but we're not on rails 4 yet.

class AddLowToBlobStoreTaskPriority < ActiveRecord::Migration[4.2]
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type ds3.blob_store_task_priority add value if not exists 'LOW';\"")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end

