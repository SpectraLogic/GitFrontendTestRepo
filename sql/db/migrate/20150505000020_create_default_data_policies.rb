require "spectra_support/cmd"

class CreateDefaultDataPolicies < ActiveRecord::Migration[4.2]

  DB_BACKUP_BUCKET = 'Spectra-BlackPearl-Backup'

  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]

    # Use these Cmd.run statements to update enums, since updating an enum cannot run inside a transaction (which the execute statements do).
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type ds3.s3_object_spanning_policy add value if not exists 'SPECTRA_PROPRIETARY';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type ds3.write_optimization add value if not exists 'CAPACITY';\"")

    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type ds3.storage_domain_member_state add value if not exists 'NORMAL';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type ds3.write_preference_level add value if not exists 'NORMAL';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type ds3.data_isolation_level add value if not exists 'BUCKET_ISOLATED';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type ds3.data_persistence_rule_state add value if not exists 'NORMAL';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type ds3.data_persistence_rule_type add value if not exists 'PERMANENT';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type ds3.bucket_acl_permission add value if not exists 'OWNER';\"")

    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"alter type ds3.ltfs_file_naming_mode add value if not exists 'OBJECT_NAME';\"")

    storage_domain_id = nil

    # Create a storage domain member for each tape partition
    tape_partitions = execute("select * from tape.tape_partition")
    if tape_partitions.to_a.size > 0
      # Only create the storage domain when we know there is a tape library
      storage_domain_id = create_storage_domain
      tape_partitions.each do |tp|
        tape_types_res = execute("SELECT DISTINCT type from tape.tape;")
        tape_types = tape_types_res.to_a.map{|t| t["type"]}
        tape_types.each do |tape_type|
          execute("INSERT INTO ds3.storage_domain_member (pool_partition_id, id, state, storage_domain_id, tape_type, tape_partition_id, write_preference) VALUES (NULL, '#{SecureRandom.uuid}', 'NORMAL', '#{storage_domain_id}', '#{tape_type}', '#{tp['id']}', 'NORMAL')")
        end
      end
      execute("UPDATE tape.tape SET storage_domain_id= '#{storage_domain_id}' WHERE assigned_to_bucket = 'true';")
    end

    # Create data policies for buckets
    single_data_policy_id = nil
    bucket_attrs = execute("SELECT default_checksum, default_get_job_priority, default_put_job_priority, default_verify_job_priority FROM ds3.bucket WHERE name != '#{DB_BACKUP_BUCKET}';").to_a

    if bucket_attrs.size > 0
      storage_domain_id ||= create_storage_domain
    end

    if bucket_attrs.uniq.size == 1
      # All buckets currently have identical data policy attributes.
      # Create a single data policy, and assign it to all of them
      single_data_policy_id = create_data_policy(bucket_attrs[0], "Data Policy for 1.x Buckets")

      # Create persistence rule
      create_persistence_rule(single_data_policy_id, storage_domain_id)
    end

    # Assign buckets to data policy (creating data policies if necessary)
    buckets = execute("SELECT id, name, user_id, default_checksum, default_get_job_priority, default_put_job_priority, default_verify_job_priority FROM ds3.bucket WHERE name != '#{DB_BACKUP_BUCKET}';").to_a
    for bucket in buckets
      if single_data_policy_id
        execute("UPDATE ds3.bucket set data_policy_id = '#{single_data_policy_id}' WHERE id = '#{bucket['id']}';")
      else
        data_policy_id = create_data_policy(bucket, "Data Policy for bucket #{bucket['name']}")
        # Create persistence rule
        create_persistence_rule(data_policy_id, storage_domain_id)
        # Assign bucket to data_policy rule
        execute("UPDATE ds3.bucket set data_policy_id = '#{data_policy_id}' WHERE id = '#{bucket['id']}';")
      end
    end

    # Add bucketAcls for the owner of each bucket
    for bucket in buckets
      execute("INSERT INTO ds3.bucket_acl (bucket_id, user_id, permission, id) VALUES ('#{bucket['id']}', '#{bucket['user_id']}', 'OWNER', '#{SecureRandom.uuid}');")
    end

    # Create policy for DB backup bucket 
    backup_bucket = execute("SELECT * from ds3.bucket WHERE name = '#{DB_BACKUP_BUCKET}';").to_a;
    if backup_bucket.size == 1
      bucket = backup_bucket[0]
      storage_domain_id ||= create_storage_domain
      data_policy_id = create_data_policy(bucket, "Spectra Database Backup Policy")
      create_persistence_rule(data_policy_id, storage_domain_id)
      execute("UPDATE ds3.bucket set data_policy_id = '#{data_policy_id}' WHERE id = '#{bucket['id']}';")
    end
  end

  def down
  end

  def create_data_policy(bucket, data_policy_name)
    data_policy_id = SecureRandom.uuid
    execute("INSERT INTO ds3.data_policy (blobbing_enabled, creation_date, checksum_type, default_get_job_priority, default_put_job_priority, default_verify_job_priority, end_to_end_crc_required, ltfs_object_naming_allowed, id, name, rebuild_priority, versioning) VALUES (true, 'now', '#{bucket['default_checksum']}', '#{bucket['default_get_job_priority']}', '#{bucket['default_put_job_priority']}', '#{bucket['default_verify_job_priority']}', 'false', true, '#{data_policy_id}', '#{data_policy_name}', 'LOW', 'NONE');");
    return data_policy_id
  end

  def create_persistence_rule(data_policy_id, storage_domain_id)
      execute("INSERT INTO ds3.data_persistence_rule (data_policy_id, isolation_level, minimum_days_to_retain, id, state, storage_domain_id, type) VALUES ('#{data_policy_id}', 'BUCKET_ISOLATED', NULL, '#{SecureRandom.uuid}', 'NORMAL', '#{storage_domain_id}', 'PERMANENT');")
  end

  def create_storage_domain
    storage_domain_id = SecureRandom.uuid
    execute("insert into ds3.storage_domain (id, name, max_tape_fragmentation_percent, maximum_auto_verification_frequency_in_days, auto_eject_upon_job_cancellation, auto_eject_upon_job_completion, auto_eject_upon_media_full, media_ejection_allowed, write_optimization, ltfs_file_naming) VALUES ('#{storage_domain_id}', 'All Storage', 65, 365, false, false, false, false, 'CAPACITY', 'OBJECT_NAME')")
    return storage_domain_id
  end

end
