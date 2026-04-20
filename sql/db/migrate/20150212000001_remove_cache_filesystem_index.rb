class RemoveCacheFilesystemIndex < ActiveRecord::Migration[4.2]
  def up
    execute("SET search_path = planner, pg_catalog");
    execute("DROP INDEX planner_cache_filesystem__node_id;")
    execute("SET search_path = public, pg_catalog;")
  end

  def down
    execute("CREATE INDEX planner_cache_filesystem__node_id ON planner.cache_filesystem (node_id);")
  end
end

