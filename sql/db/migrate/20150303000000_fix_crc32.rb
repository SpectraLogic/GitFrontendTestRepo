require "spectra_support/cmd"

class FixCrc32 < ActiveRecord::Migration[4.2]
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type security.checksum_type add value if not exists 'CRC';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type security.checksum_type add value if not exists 'CRC_32';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"UPDATE ds3.blob set checksum_type='CRC_32' where checksum_type='CRC';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"UPDATE ds3.bucket set default_checksum='CRC_32' where default_checksum='CRC';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"UPDATE ds3.bucket set min_client_checksum='CRC_32' where min_client_checksum='CRC';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"UPDATE ds3.bucket set max_client_checksum='CRC_32' where max_client_checksum='CRC';\"")
  end

  def down
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"UPDATE ds3.blob set checksum_type='CRC' where checksum_type='CRC_32';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"UPDATE ds3.bucket set default_checksum='CRC' where default_checksum='CRC_32';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"UPDATE ds3.bucket set min_client_checksum='CRC' where min_client_checksum='CRC_32';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"UPDATE ds3.bucket set max_client_checksum='CRC' where max_client_checksum='CRC_32';\"")
  end
end
