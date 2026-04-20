class SetAutoQuiesceTrue < ActiveRecord::Migration[4.2]
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]

    execute("UPDATE tape.tape_partition SET auto_quiesce_enabled = 't';")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end
