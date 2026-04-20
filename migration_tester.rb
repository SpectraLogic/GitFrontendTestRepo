require 'pg'
require 'spectra_support/cmd'
require 'securerandom'
require 'test/unit'

class MigrationTester < Test::Unit::TestCase

  def setup
    @rakefile = ARGV[0] || '../mgmt/sql/Rakefile'
    @db_name = 'migration_test_db'
    @user = 'Administrator'
    puts "Creating Databases"
    `createdb -U #{@user} #{@db_name}`
    @d = PG.connect( :dbname => "#{@db_name}", :user => @user)
  end
  
  def teardown
    @d.close unless @d.nil?
    `dropdb -U #{@user} --if-exists #{@db_name}`
  end

  def run_initial_migration
    puts "Migrating initial database to 1.x release"
    migration_out = `TAPESYSTEM_DB_NAME=#{@db_name} rake -f #{@rakefile} db:migrate VERSION=20150302000001`
    puts "Output of migration:"
    puts migration_out
  end

  def populate_database
    puts "Populating database at 1.x level"
    puts "Adding necesssary enums"
    Cmd.run!("/usr/local/bin/psql -U #{@user} #{@db_name} -c \"alter type tape.tape_partition_quiesced add value if not exists 'NO';\"")
    Cmd.run!("/usr/local/bin/psql -U #{@user} #{@db_name} -c \"alter type tape.tape_partition_state add value if not exists 'ONLINE';\"")
    Cmd.run!("/usr/local/bin/psql -U #{@user} #{@db_name} -c \"alter type security.checksum_type add value if not exists 'MD5';\"")
    Cmd.run!("/usr/local/bin/psql -U #{@user} #{@db_name} -c \"alter type ds3.blob_store_task_priority add value if not exists 'NORMAL';\"")
    Cmd.run!("/usr/local/bin/psql -U #{@user} #{@db_name} -c \"alter type ds3.blob_store_task_priority add value if not exists 'HIGH';\"")
    Cmd.run!("/usr/local/bin/psql -U #{@user} #{@db_name} -c \"alter type ds3.write_optimization add value if not exists 'PERFORMANCE';\"")
    Cmd.run!("/usr/local/bin/psql -U #{@user} #{@db_name} -c \"alter type ds3.write_optimization add value if not exists 'CAPACITY';\"")
    Cmd.run!("/usr/local/bin/psql -U #{@user} #{@db_name} -c \"alter type security.checksum_type add value if not exists 'SHA_256';\"")
    Cmd.run!("/usr/local/bin/psql -U #{@user} #{@db_name} -c \"alter type ds3.s3_object_spanning_policy add value if not exists 'SPECTRA_PROPRIETARY';\"")

    puts "Adding tape partition"
    @d.exec("INSERT INTO tape.tape_library VALUES ('344c5bae-a561-463a-978d-6864ea7810e5', '', '', 'SN')")
    @d.exec("INSERT INTO tape.tape_partition VALUES ('', '344c5bae-a561-463a-978d-6864ea7810e4', '344c5bae-a561-463a-978d-6864ea7810e5', '9112005128', 'NO', '9112005128', 'ONLINE', 'NOT_SUPPORTED')")

    @user1 = SecureRandom.uuid
    @d.exec("INSERT INTO ds3.\"user\" VALUES ('foo', '#{@user1}', 'user1', 'bar')")
  end

  def add_two_similar_buckets
    @d.exec("INSERT INTO ds3.bucket VALUES('now', 'MD5', 'NORMAL', 'NORMAL', 'PERFORMANCE', '1a342b34-d36d-47b0-b663-0bf3726e61f0', 'SHA_256', NULL, 'bucket32212254720', 'SPECTRA_PROPRIETARY', '#{@user1}');")
    @d.exec("INSERT INTO ds3.bucket VALUES('now', 'MD5', 'NORMAL', 'NORMAL', 'PERFORMANCE', '1a342b34-d36d-47b0-b663-0bf3726e61f1', 'SHA_256', NULL, 'bucket3221225472', 'SPECTRA_PROPRIETARY', '#{@user1}');")
  end

  def add_two_different_buckets
    @d.exec("INSERT INTO ds3.bucket VALUES('now', 'SHA_256', 'NORMAL', 'NORMAL', 'PERFORMANCE', '1a342b34-d36d-47b0-b663-0bf3726e61f0', 'SHA_256', NULL, 'bucket32212254720', 'SPECTRA_PROPRIETARY', '#{@user1}');")
    @d.exec("INSERT INTO ds3.bucket VALUES('now', 'MD5', 'NORMAL', 'NORMAL', 'PERFORMANCE', '1a342b34-d36d-47b0-b663-0bf3726e61f1', 'SHA_256', NULL, 'bucket3221225472', 'SPECTRA_PROPRIETARY', '#{@user1}');")
  end

  def add_backup_bucket
    @d.exec("INSERT INTO ds3.bucket VALUES('now', 'MD5', 'HIGH', 'HIGH', 'CAPACITY', '1a342b34-d36d-47b0-b663-0bf3726e61f2', NULL, NULL, 'Spectra-BlackPearl-Backup', 'SPECTRA_PROPRIETARY', '#{@user1}');")
  end

  def migrate_to_two
    puts "Migrating initial database to 1.x release"
    migration_out = `TAPESYSTEM_DB_NAME=#{@db_name} rake -f #{@rakefile} db:migrate`
    puts "Output of migration:"
    puts migration_out
  end

  def assert_one_sd
    res = @d.exec("SELECT * FROM ds3.storage_domain").to_a
    assert_equal 1, res.size
  end

  def test_different_buckets
    run_initial_migration
    populate_database
    add_two_different_buckets
    migrate_to_two
    res = @d.exec("SELECT * FROM ds3.data_policy").to_a
    assert_equal 2, res.size
    assert_one_sd
  end

  def test_similar_buckets
    run_initial_migration
    populate_database
    add_two_similar_buckets
    migrate_to_two
    res = @d.exec("SELECT * FROM ds3.data_policy").to_a
    assert_equal 1, res.size
    assert_one_sd
  end

  def test_no_buckets
    run_initial_migration
    populate_database
    migrate_to_two
    res = @d.exec("SELECT * FROM ds3.data_policy").to_a
    assert_equal 0, res.size
    assert_one_sd
  end

  def test_with_just_backup_bucket
    run_initial_migration
    populate_database
    add_backup_bucket
    migrate_to_two
    res = @d.exec("SELECT * FROM ds3.data_policy").to_a
    assert_equal 1, res.size
    assert_one_sd
  end

  def test_with_backup_and_other_buckets
    run_initial_migration
    populate_database
    add_two_similar_buckets
    add_backup_bucket
    migrate_to_two
    res = @d.exec("SELECT * FROM ds3.data_policy").to_a
    puts res
    assert_equal 2, res.size
    res = @d.exec("SELECT * FROM ds3.bucket").to_a
    assert_equal 3, res.size
    assert_one_sd
  end

  def test_no_libraries_no_buckets
    # Should not create any storage domains or data policies
    run_initial_migration
    migrate_to_two
    res = @d.exec("SELECT * FROM ds3.data_policy").to_a
    assert_equal 0, res.size
    res = @d.exec("SELECT * FROM ds3.storage_domain").to_a
    assert_equal 0, res.size
    res = @d.exec("SELECT * FROM ds3.storage_domain_member").to_a
    assert_equal 0, res.size
    res = @d.exec("SELECT * FROM ds3.data_persistence_rule").to_a
    assert_equal 0, res.size
  end
   
end
