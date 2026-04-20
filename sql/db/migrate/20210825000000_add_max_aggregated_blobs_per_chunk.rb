class AddMaxAggregatedBlobsPerChunk < ActiveRecord::Migration[4.2]
  def up
    execute("ALTER TABLE ds3.data_path_backend ADD COLUMN IF NOT EXISTS max_aggregated_blobs_per_chunk INTEGER;")
    execute("UPDATE ds3.data_path_backend SET max_aggregated_blobs_per_chunk = 20000;")
    execute("ALTER TABLE ds3.data_path_backend ALTER COLUMN max_aggregated_blobs_per_chunk SET NOT NULL;")
  end

  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end
