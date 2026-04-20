class RemoveJobChunks < ActiveRecord::Migration[4.2]
  def up
    #fail migration if there are any active jobs:
    if execute("SELECT COUNT(*) FROM ds3.job").first.values.first.to_i > 0
      raise("Cannot run migration while there are active jobs")
    end

    execute("ALTER TABLE ds3.job_chunk ADD COLUMN IF NOT EXISTS blob_id UUID NOT NULL REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE;")
    execute("DROP TABLE ds3.job_entry;")
    execute("ALTER TABLE ds3.job_chunk RENAME TO job_entry;")

    execute("ALTER TABLE ds3.job_entry RENAME CONSTRAINT job_chunk_read_from_tape_id_fkey TO job_entry_read_from_tape_id_fkey;")
    execute("ALTER TABLE ds3.job_entry RENAME CONSTRAINT job_chunk_read_from_pool_id_fkey TO job_entry_read_from_pool_id_fkey;")
    execute("ALTER TABLE ds3.job_entry RENAME CONSTRAINT job_chunk_read_from_s3_target_id_fkey TO job_entry_read_from_s3_target_id_fkey;")
    execute("ALTER TABLE ds3.job_entry RENAME CONSTRAINT job_chunk_read_from_ds3_target_id_fkey TO job_entry_read_from_ds3_target_id_fkey;")
    execute("ALTER TABLE ds3.job_entry RENAME CONSTRAINT job_chunk_read_from_azure_target_id_fkey TO job_entry_read_from_azure_target_id_fkey;")

    execute("ALTER TABLE ds3.job_chunk_ds3_target ADD COLUMN IF NOT EXISTS blob_store_state ds3.job_chunk_blob_store_state NOT NULL;")
    execute("ALTER TABLE ds3.job_chunk_s3_target ADD COLUMN IF NOT EXISTS blob_store_state ds3.job_chunk_blob_store_state NOT NULL;")
    execute("ALTER TABLE ds3.job_chunk_azure_target ADD COLUMN IF NOT EXISTS blob_store_state ds3.job_chunk_blob_store_state NOT NULL;")
    execute("ALTER TABLE ds3.job_chunk_persistence_target ADD COLUMN IF NOT EXISTS blob_store_state ds3.job_chunk_blob_store_state NOT NULL;")

    execute("ALTER TABLE ds3.job_chunk_ds3_target DROP COLUMN IF EXISTS done;")
    execute("ALTER TABLE ds3.job_chunk_s3_target DROP COLUMN IF EXISTS done;")
    execute("ALTER TABLE ds3.job_chunk_azure_target DROP COLUMN IF EXISTS done;")
    execute("ALTER TABLE ds3.job_chunk_persistence_target DROP COLUMN IF EXISTS done;")

    execute("ALTER TABLE ds3.job_chunk_ds3_target ADD COLUMN IF NOT EXISTS rule_id UUID NOT NULL;")
    execute("ALTER TABLE ds3.job_chunk_s3_target ADD COLUMN IF NOT EXISTS rule_id UUID NOT NULL;")
    execute("ALTER TABLE ds3.job_chunk_azure_target ADD COLUMN IF NOT EXISTS rule_id UUID NOT NULL;")
    execute("ALTER TABLE ds3.job_chunk_persistence_target ADD COLUMN IF NOT EXISTS persistence_rule_id UUID REFERENCES ds3.data_persistence_rule ON UPDATE CASCADE ON DELETE SET NULL;")

    execute("ALTER TABLE ds3.job_chunk_persistence_target ADD COLUMN IF NOT EXISTS isolated_bucket_id UUID REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE;")

    execute("ALTER TABLE ds3.job_chunk_persistence_target RENAME TO local_blob_destination;")
    execute("ALTER TABLE ds3.job_chunk_azure_target RENAME TO azure_blob_destination;")
    execute("ALTER TABLE ds3.job_chunk_s3_target RENAME TO s3_blob_destination;")
    execute("ALTER TABLE ds3.job_chunk_ds3_target RENAME TO ds3_blob_destination;")

    # add indices for new fields

    execute('CREATE INDEX ON ds3.local_blob_destination (isolated_bucket_id);')
    execute('CREATE INDEX ON ds3.local_blob_destination (persistence_rule_id);')

    execute('CREATE INDEX ON ds3.job_entry (blob_id);')
    execute('CREATE INDEX ON ds3.job (priority);')

    execute("ALTER TABLE ds3.bucket DROP COLUMN IF EXISTS preferred_chunk_size_in_bytes;")

    #Rename chunk ID to entry ID
    execute("ALTER TABLE ds3.local_blob_destination RENAME COLUMN chunk_id TO entry_id;")
    execute("ALTER TABLE ds3.s3_blob_destination RENAME COLUMN chunk_id TO entry_id;")
    execute("ALTER TABLE ds3.azure_blob_destination RENAME COLUMN chunk_id TO entry_id;")
    execute("ALTER TABLE ds3.ds3_blob_destination RENAME COLUMN chunk_id TO entry_id;")

    #rename restore to iom_type and change enum values, only if they already exist
    execute("ALTER TABLE ds3.job RENAME COLUMN restore TO iom_type;")
    execute("ALTER TYPE ds3.job_restore RENAME TO iom_type;")
    execute("ALTER TYPE ds3.iom_type RENAME VALUE 'NO' TO 'NONE';")
    execute("ALTER TYPE ds3.iom_type RENAME VALUE 'YES' TO 'STAGE';")
    execute("ALTER TYPE ds3.iom_type RENAME VALUE 'PERMANENT_ONLY' TO 'STANDARD_IOM';")

    execute("ALTER TABLE ds3.job_entry ADD COLUMN IF NOT EXISTS chunk_id UUID;")

    # add views for joins:
    execute("CREATE VIEW ds3.detailed_job_entry AS" +
            " SELECT job_entry.*, job.request_type, job.priority, job.created_at, job.iom_type, job.bucket_id, blob.object_id, blob.length, blob_cache.state as cache_state, blob_cache.size_in_bytes as cache_size_in_bytes" +
            " FROM ds3.job_entry" +
            " JOIN ds3.job on job_id = job.id" +
            " JOIN ds3.blob on blob_id = blob.id" +
            " LEFT JOIN planner.blob_cache on blob_cache.blob_id = blob.id;")

    execute("CREATE VIEW ds3.detailed_local_blob_destination AS" +
            " SELECT local_blob_destination.*, job.request_type, job.iom_type, job.bucket_id, job_entry.job_id, job_entry.chunk_number, job.priority, job.created_at" +
            " FROM ds3.local_blob_destination" +
            " JOIN ds3.job_entry on entry_id = job_entry.id" +
            " JOIN ds3.job on job_entry.job_id = job.id;")

    execute("CREATE VIEW ds3.detailed_s3_blob_destination AS" +
            " SELECT s3_blob_destination.*, job_entry.job_id, job.priority, job.created_at" +
            " FROM ds3.s3_blob_destination" +
            " JOIN ds3.job_entry on entry_id = job_entry.id" +
            " JOIN ds3.job on job_entry.job_id = job.id;")

    execute("CREATE VIEW ds3.detailed_azure_blob_destination AS" +
            " SELECT azure_blob_destination.*, job_entry.job_id, job.priority, job.created_at" +
            " FROM ds3.azure_blob_destination" +
            " JOIN ds3.job_entry on entry_id = job_entry.id" +
            " JOIN ds3.job on job_entry.job_id = job.id;")

    execute("CREATE VIEW ds3.detailed_ds3_blob_destination AS" +
            " SELECT ds3_blob_destination.*, job_entry.job_id, job.priority, job.created_at" +
            " FROM ds3.ds3_blob_destination" +
            " JOIN ds3.job_entry on entry_id = job_entry.id" +
            " JOIN ds3.job on job_entry.job_id = job.id;")

    execute("CREATE VIEW ds3.local_job_entry_work AS" +
            " SELECT job_entry.*, job.request_type, job.priority, job.created_at, job.iom_type, job.bucket_id, blob.object_id, blob.length, blob_cache.state as cache_state, local_blob_destination.blob_store_state as destination_state, local_blob_destination.storage_domain_id, local_blob_destination.isolated_bucket_id, local_blob_destination.persistence_rule_id, blob_tape.order_index, local_blob_destination.id as local_blob_destination_id" +
            " FROM ds3.job_entry" +
            " LEFT JOIN ds3.local_blob_destination on entry_id = job_entry.id" +
            " JOIN ds3.job on job_id = job.id" +
            " JOIN ds3.blob on blob_id = blob.id" +
            " LEFT JOIN tape.blob_tape on blob.id = blob_tape.blob_id AND blob_tape.tape_id = read_from_tape_id" +
            " LEFT JOIN planner.blob_cache on blob_cache.blob_id = blob.id;")

    execute("CREATE VIEW ds3.s3_job_entry_work AS" +
            " SELECT job_entry.*, job.request_type, job.priority, job.created_at, job.iom_type, job.bucket_id, blob.object_id, blob.length, blob_cache.state as cache_state, s3_blob_destination.blob_store_state as destination_state, s3_blob_destination.target_id, s3_blob_destination.rule_id, s3_blob_destination.id as s3_blob_destination_id" +
            " FROM ds3.job_entry" +
            " LEFT JOIN ds3.s3_blob_destination on entry_id = job_entry.id" +
            " JOIN ds3.job on job_id = job.id" +
            " JOIN ds3.blob on blob_id = blob.id" +
            " LEFT JOIN planner.blob_cache on blob_cache.blob_id = blob.id;")

    execute("CREATE VIEW ds3.azure_job_entry_work AS" +
            " SELECT job_entry.*, job.request_type, job.priority, job.created_at, job.iom_type, job.bucket_id, blob.object_id, blob.length, blob_cache.state as cache_state, azure_blob_destination.blob_store_state as destination_state, azure_blob_destination.target_id, azure_blob_destination.rule_id, azure_blob_destination.id as azure_blob_destination_id" +
            " FROM ds3.job_entry" +
            " LEFT JOIN ds3.azure_blob_destination on entry_id = job_entry.id" +
            " JOIN ds3.job on job_id = job.id" +
            " JOIN ds3.blob on blob_id = blob.id" +
            " LEFT JOIN planner.blob_cache on blob_cache.blob_id = blob.id;")

    execute("CREATE VIEW ds3.ds3_job_entry_work AS" +
            " SELECT job_entry.*, job.request_type, job.priority, job.created_at, job.iom_type, job.bucket_id, blob.object_id, blob.length, blob_cache.state as cache_state, ds3_blob_destination.blob_store_state as destination_state, ds3_blob_destination.target_id, ds3_blob_destination.rule_id, ds3_blob_destination.id as ds3_blob_destination_id" +
            " FROM ds3.job_entry" +
            " LEFT JOIN ds3.ds3_blob_destination on entry_id = job_entry.id" +
            " JOIN ds3.job on job_id = job.id" +
            " JOIN ds3.blob on blob_id = blob.id" +
            " LEFT JOIN planner.blob_cache on blob_cache.blob_id = blob.id;")

    execute("ALTER TABLE ds3.data_path_backend ADD COLUMN IF NOT EXISTS emulate_chunks boolean;")
    execute("update ds3.data_path_backend set emulate_chunks=false;")
    execute("alter table ds3.data_path_backend alter column emulate_chunks set not null;")

  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end