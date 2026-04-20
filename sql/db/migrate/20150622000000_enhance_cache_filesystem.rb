class EnhanceCacheFilesystem < ActiveRecord::Migration[4.2]
  def up
    execute("UPDATE planner.cache_filesystem SET max_percent_utilization_of_filesystem=0.9")
    
    execute("ALTER TABLE planner.cache_filesystem ADD COLUMN auto_reclaim_initiate_threshold double precision")
    execute("UPDATE planner.cache_filesystem SET auto_reclaim_initiate_threshold=0.82")
    execute("ALTER TABLE planner.cache_filesystem ALTER COLUMN auto_reclaim_initiate_threshold SET NOT NULL")
    
    execute("ALTER TABLE planner.cache_filesystem ADD COLUMN auto_reclaim_terminate_threshold double precision")
    execute("UPDATE planner.cache_filesystem SET auto_reclaim_terminate_threshold=0.72")
    execute("ALTER TABLE planner.cache_filesystem ALTER COLUMN auto_reclaim_terminate_threshold SET NOT NULL")
    
    execute("ALTER TABLE planner.cache_filesystem ADD COLUMN burst_threshold double precision")
    execute("UPDATE planner.cache_filesystem SET burst_threshold=0.85;")
    execute("ALTER TABLE planner.cache_filesystem ALTER COLUMN burst_threshold SET NOT NULL")
  end

  def down
    execute("ALTER TABLE planner.cache_filesystem DROP COLUMN auto_reclaim_initiate_threshold")  
    execute("ALTER TABLE planner.cache_filesystem DROP COLUMN auto_reclaim_terminate_threshold")  
    execute("ALTER TABLE planner.cache_filesystem DROP COLUMN burst_threshold")  
  end
end
