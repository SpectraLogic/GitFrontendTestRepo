require "spectra_support/cmd"

class AddPartitionSerialId < ActiveRecord::Migration[4.2]
  SQL_FILE = File.expand_path(__FILE__).sub(/rb$/, "sql")

  def up
    execute('alter table tape.tape_partition add column serial_id varchar;')
    tape_partitions = execute('select id, serial_number from tape.tape_partition')
    tape_partitions.to_a.each do |row|
      id = row['id']
      serial_number = row['serial_number']
      serial_id = serial_number
      #We mask the serial in order to produce a reliable unique identifier (see BLKP-2743)
      serial_id[0] = '0'
      serial_id[2] = '0'
      serial_id[3] = '0'
      execute("update tape.tape_partition set serial_id='#{serial_id}' where id='#{id}';")
    end
    execute('alter table tape.tape_partition alter column serial_id set not null;')
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end
