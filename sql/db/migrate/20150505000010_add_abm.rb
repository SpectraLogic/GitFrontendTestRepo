require "spectra_support/cmd"

class AddAbm < ActiveRecord::Migration[4.2]
  SQL_FILE = File.expand_path(__FILE__).sub(/rb$/, "sql")

  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -f #{SQL_FILE} --single-transaction -v ON_ERROR_STOP=1")
    
    execute("alter table ds3.data_policy add column versioning ds3.versioning_level;")
    execute("update ds3.data_policy set versioning='NONE';")
    execute("alter table ds3.data_policy alter column versioning set not null;")
    
    execute("alter table ds3.s3_object add column latest boolean;")
    execute("update ds3.s3_object set latest=true;")
    execute("alter table ds3.s3_object alter column latest set not null;")
    
    execute("alter table tape.tape_partition add column drive_type tape.tape_drive_type;")
    execute("alter table ds3.node add column dns_name varchar;")
    execute("alter table ds3.bucket add column last_preferred_chunk_size_in_bytes bigint;")
    
    execute("alter table ds3.job add column reshapable boolean;")
    execute("update ds3.job set reshapable=false;")
    execute("alter table ds3.job alter column reshapable set not null;")
    
    execute("alter table ds3.job add column truncated boolean;")
    execute("update ds3.job set truncated=false;")
    execute("alter table ds3.job alter column truncated set not null;")
    
    execute("alter table ds3.completed_job add column truncated boolean;")
    execute("update ds3.completed_job set truncated=false;")
    execute("alter table ds3.completed_job alter column truncated set not null;")
    
    execute("alter table ds3.canceled_job add column truncated boolean;")
    execute("update ds3.canceled_job set truncated=false;")
    execute("alter table ds3.canceled_job alter column truncated set not null;")
    
    execute("alter table ds3.job add column rechunked timestamp without time zone;")
    execute("alter table ds3.completed_job add column rechunked timestamp without time zone;")
    execute("alter table ds3.canceled_job add column rechunked timestamp without time zone;")
    
    execute("ALTER TABLE ds3.job ADD COLUMN error_message varchar;")
    execute("ALTER TABLE ds3.completed_job ADD COLUMN error_message varchar;")
    execute("ALTER TABLE ds3.canceled_job ADD COLUMN error_message varchar;")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end

