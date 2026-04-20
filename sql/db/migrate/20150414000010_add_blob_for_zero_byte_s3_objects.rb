require 'securerandom' # generate uuid
require 'tempfile'

class AddBlobForZeroByteS3Objects < ActiveRecord::Migration[4.2]
  def up
    create_blobs_file
    fill_blobs_file
    close_blobs_file

    execute("COPY ds3.blob FROM \'#{@fd.path}\';")
  rescue => e
    puts("Exception during AddBlobForZeroByteS3Objects up migrate: #{e}")
    raise e
  ensure
    teardown
  end

  def down
    execute("DELETE FROM ds3.blob WHERE length = 0;")
  end

  private

  def fill_blobs_file
    get_array_from_s3object_ids_query
    @s3_object_ids_array.each do |s3_object_id|
      add_blob_entry_to_blobs_file(s3_object_id)
    end
  end

  def get_array_from_s3object_ids_query
    query_output = find_s3_objects_without_blobs
    @s3_object_ids_array = []

    query_output.each_line do |line|
      next if line.length < 32
      @s3_object_ids_array.push line.strip
    end
  end

  def find_s3_objects_without_blobs
    psql_exec('SELECT id FROM ds3.s3_object '\
              'WHERE NOT EXISTS '\
              '(SELECT * FROM ds3.blob WHERE object_id = ds3.s3_object.id);')
  end

  def add_blob_entry_to_blobs_file(object_id)
    @fd << "0" << "\t"
    @fd << "NULL" << "\t" # checksum
    @fd << "MD5" << "\t" # checksum_type
    @fd << SecureRandom.uuid << "\t"
    @fd << "0" << "\t"
    @fd << object_id << "\n"
  end

  def psql_exec( cmd )
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]

    psql_cmd = "psql -U #{user} -d #{db} -t -c \"#{cmd}\" "
    exec_output = `#{psql_cmd} 2>&1`

    if $?.to_i != 0 # check backticks return code
      puts "\nSQL ERROR !!! [#{$?.to_s}]"
      puts psql_cmd
      puts exec_output
      raise RuntimeError
    elsif $debug
      puts psql_cmd
      puts exec_output
    end

    return exec_output
  end

  def create_blobs_file
    @fd = Tempfile.new("add_block_for_zero_byte_s3_objects_migration")
    @fd.chmod(0644)
  end

  def close_blobs_file
    @fd.close
  end

  def teardown
    filename = @fd.path
    @fd.unlink
  rescue => e
    puts "Delete file #{filename} exception: #{e}"
    raise e
  end
end
