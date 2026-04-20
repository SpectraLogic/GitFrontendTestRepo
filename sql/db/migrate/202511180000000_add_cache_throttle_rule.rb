require 'securerandom'
require 'spectra_support/cmd'

class AddCacheThrottleRule < ActiveRecord::Migration[4.2]
  def up
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    user = config[:username]
    db = config[:database]
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"ALTER TYPE ds3.blob_store_task_priority add value if not exists 'NORMAL';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"ALTER TYPE ds3.blob_store_task_priority add value if not exists 'LOW';\"")
    Cmd.run!("/usr/local/bin/psql -U #{user} #{db} -c \"ALTER TYPE ds3.blob_store_task_priority add value if not exists 'BACKGROUND';\"")

    execute("CREATE TABLE ds3.cache_throttle_rule (
      id                uuid                         NOT NULL,
      priority          ds3.blob_store_task_priority NULL,
      bucket_id         uuid                         NULL REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
      request_type      ds3.job_request_type         NULL,
      max_cache_percent double precision             NOT NULL,
      burst_threshold   double precision             NULL,
      PRIMARY KEY (id),
      UNIQUE (priority, bucket_id, request_type)
    );")

    execute('CREATE INDEX ON ds3.cache_throttle_rule (bucket_id);')
    execute("INSERT INTO ds3.cache_throttle_rule (id, priority, max_cache_percent, burst_threshold) VALUES ('#{SecureRandom.uuid}', 'NORMAL', .95, .85);")
    execute("INSERT INTO ds3.cache_throttle_rule (id, priority, max_cache_percent, burst_threshold) VALUES ('#{SecureRandom.uuid}', 'LOW', .85, .85);")
    execute("INSERT INTO ds3.cache_throttle_rule (id, priority, max_cache_percent, burst_threshold) VALUES ('#{SecureRandom.uuid}', 'BACKGROUND', .75, .85);")

    execute("CREATE VIEW ds3.pg_stat_all_tables AS " +
            "select n_tup_del as job_entry_n_tup_del from pg_stat_all_tables where relname='job_entry';")
  end

  def down
    execute("DROP table ds3.cache_throttle_rule;")
    execute("DROP VIEW IF EXISTS ds3.cache_throttle_rule;")
  end
end