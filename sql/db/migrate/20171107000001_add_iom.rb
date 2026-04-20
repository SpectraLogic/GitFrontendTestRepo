require "spectra_support/cmd"

class AddIom < ActiveRecord::Migration[4.2]
SQL_FILE = File.expand_path(__FILE__).sub(/rb$/, "sql")
  def up 
    execute('alter table tape.tape add column storage_domain_member_id uuid NULL REFERENCES ds3.storage_domain_member ON UPDATE CASCADE ON DELETE SET NULL')
    execute('alter table pool.pool add column storage_domain_member_id uuid NULL REFERENCES ds3.storage_domain_member ON UPDATE CASCADE ON DELETE SET NULL')
    tapes = execute('select id, bar_code, partition_id, type, storage_domain_id from tape.tape where storage_domain_id is not NULL')
    pools = execute('select id, name, partition_id, storage_domain_id from pool.pool where storage_domain_id is not NULL')
    
    tapes.to_a.each do |row|
      id = row['id']
      bc = row['bar_code']
      p_id = row['partition_id']
      sd_id = row['storage_domain_id']
      type = row['type']
      if (sd_id)
        sdm_ids = execute("select id from ds3.storage_domain_member where storage_domain_id='#{sd_id}' and tape_partition_id='#{p_id}' and tape_type='#{type}';")
        if sdm_ids.to_a.size != 1
          raise "Failed to select an appropriate storage domain member to assign tape '#{id}' ('#{bc}') to for storage domain '#{sd_id}'"
        end
        sdm_id = sdm_ids.to_a[0]['id']
        execute("update tape.tape set storage_domain_member_id='#{sdm_id}' where id='#{id}';")
      else
        raise "Failed to select an appropriate storage domain member to assign tape '#{id}' ('#{bc}') to for storage domain '#{sd_id}'"  
      end
    end
    pools.to_a.each do |row|
      id = row['id']
      name = row['name']
      p_id = row['partition_id']
      sd_id = row['storage_domain_id']
      if (sd_id)
        sdm_ids = execute("select (id) from ds3.storage_domain_member where storage_domain_id='#{sd_id}' and pool_partition_id='#{p_id}';")
        if sdm_ids.to_a.size != 1
          raise "Failed to select an appropriate storage domain member to assign pool '#{id}' ('#{name}') to for storage domain '#{sd_id}'"
        end
        sdm_id = sdm_ids.to_a[0]['id']
        execute("update pool.pool set storage_domain_member_id='#{sdm_id}' where id='#{id}';")
      else
        raise "Failed to select an appropriate storage domain member to assign pool '#{id}' ('#{name}') to for storage domain '#{sd_id}'"
      end
    end
    execute('alter table tape.tape drop column storage_domain_id')
    execute('alter table pool.pool drop column storage_domain_id')
    
    execute('CREATE INDEX ON tape.tape (storage_domain_member_id)')
    execute('CREATE INDEX ON pool.pool (storage_domain_member_id)')
    
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -f #{SQL_FILE} --single-transaction -v ON_ERROR_STOP=1")
    execute("CREATE TABLE tape.obsolete_blob_tape (
          blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
          id               uuid                   NOT NULL   REFERENCES tape.blob_tape ON UPDATE CASCADE ON DELETE CASCADE,
          order_index      integer                NOT NULL,
          tape_id          uuid                   NOT NULL   REFERENCES tape.tape ON UPDATE CASCADE,
          obsoletion_id    uuid                   NOT NULL   REFERENCES ds3.obsoletion ON UPDATE CASCADE ON DELETE CASCADE,

          PRIMARY KEY (id),
          UNIQUE (obsoletion_id, blob_id)
        )")
    execute('CREATE INDEX ON tape.obsolete_blob_tape (blob_id)')
    execute('CREATE INDEX ON tape.obsolete_blob_tape (tape_id)')
    execute('CREATE INDEX ON tape.obsolete_blob_tape (obsoletion_id)')

    execute("CREATE TABLE pool.obsolete_blob_pool (
	  blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
          bucket_id        uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
          date_written     timestamp without time zone NOT NULL,
          id               uuid                   NOT NULL   REFERENCES pool.blob_pool ON UPDATE CASCADE ON DELETE CASCADE,
          last_accessed    timestamp without time zone NOT NULL,
	  pool_id          uuid                   NOT NULL   REFERENCES pool.pool ON UPDATE CASCADE,
          obsoletion_id    uuid                   NOT NULL   REFERENCES ds3.obsoletion ON UPDATE CASCADE ON DELETE CASCADE,

          PRIMARY KEY (id),
          UNIQUE (obsoletion_id, blob_id)
        )")
    execute('CREATE INDEX ON pool.obsolete_blob_pool (blob_id)')
    execute('CREATE INDEX ON pool.obsolete_blob_pool (bucket_id)')
    execute('CREATE INDEX ON pool.obsolete_blob_pool (pool_id)')
    execute('CREATE INDEX ON pool.obsolete_blob_pool (obsoletion_id)')
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end
