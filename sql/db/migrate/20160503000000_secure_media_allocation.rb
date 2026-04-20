require "spectra_support/cmd"

class SecureMediaAllocation < ActiveRecord::Migration[4.2]
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
      
    execute("ALTER TABLE tape.tape ADD COLUMN tbid UUID NULL;")
    execute("UPDATE tape.tape set tbid = bucket_id;")
    execute("ALTER TABLE tape.tape DROP COLUMN bucket_id;")
    execute("ALTER TABLE tape.tape ADD COLUMN bucket_id UUID REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE SET NULL;")
    execute("UPDATE tape.tape set bucket_id = tbid;")
    execute("ALTER TABLE tape.tape DROP COLUMN tbid;")
    execute("CREATE INDEX ON tape.tape (bucket_id);")
    
    execute("ALTER TABLE pool.pool ADD COLUMN tbid UUID NULL;")
    execute("UPDATE pool.pool set tbid = bucket_id;")
    execute("ALTER TABLE pool.pool DROP COLUMN bucket_id;")
    execute("ALTER TABLE pool.pool ADD COLUMN bucket_id UUID REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE SET NULL;")
    execute("UPDATE pool.pool set bucket_id = tbid;")
    execute("ALTER TABLE pool.pool DROP COLUMN tbid;")
    execute("CREATE INDEX ON pool.pool (bucket_id);")
    
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"ALTER TYPE ds3.data_isolation_level ADD VALUE IF NOT EXISTS 'SECURE_BUCKET_ISOLATED';\"")
    execute("ALTER TABLE ds3.storage_domain ADD COLUMN secure_media_allocation boolean NULL;")
    execute("UPDATE ds3.storage_domain set secure_media_allocation=false;")
    execute("ALTER TABLE ds3.storage_domain alter column secure_media_allocation set not null;")
    execute("UPDATE ds3.storage_domain set secure_media_allocation=true where exists (select * from ds3.data_persistence_rule where storage_domain_id = ds3.storage_domain.id and isolation_level = 'SECURE_BUCKET_ISOLATED');")
    execute("UPDATE ds3.data_persistence_rule set isolation_level='BUCKET_ISOLATED' where isolation_level='SECURE_BUCKET_ISOLATED';")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end

