class AddBlobCaches < ActiveRecord::Migration[4.2]
  def up
    #mark that reconcile is needed
    execute("ALTER TABLE planner.cache_filesystem ADD COLUMN IF NOT EXISTS needs_reconcile boolean")
    execute("UPDATE planner.cache_filesystem SET needs_reconcile=true")
    execute("ALTER TABLE planner.cache_filesystem ALTER COLUMN needs_reconcile SET NOT NULL")

    #add entry state type
    execute("CREATE TYPE planner.cache_entry_state AS ENUM ()")
    execute("ALTER TYPE planner.cache_entry_state add value if not exists 'ALLOCATED'")
    execute("ALTER TYPE planner.cache_entry_state add value if not exists 'IN_CACHE'")

    #create blob_cache table for storing cache entries
    execute("CREATE TABLE planner.blob_cache (
      id               uuid                        NOT NULL,
      blob_id          uuid                        NULL REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE SET NULL,
      last_accessed    timestamp without time zone NOT NULL,
      size_in_bytes    bigint                      NOT NULL,
      state            planner.cache_entry_state   NOT NUlL,
      path             varchar                     NOT NUlL,

      PRIMARY KEY (id),
      UNIQUE (blob_id)
    );")
    execute("CREATE INDEX ON planner.blob_cache (last_accessed)")
  end

  def down
    execute("DROP table planner.blob_cache;")
    execute("drop type planner.cache_entry_state;")
    execute("ALTER TABLE planner.cache_filesystem DROP COLUMN needs_reconcile")
  end
end
