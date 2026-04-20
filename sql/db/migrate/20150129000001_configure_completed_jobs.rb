class ConfigureCompletedJobs < ActiveRecord::Migration[4.2]
  def up
    execute("CREATE TABLE ds3.completed_job (
            bucket_id        uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
            cached_size_in_bytes bigint             NOT NULL,
            chunk_client_processing_order_guarantee ds3.job_chunk_client_processing_order_guarantee NOT NULL,
            completed_size_in_bytes bigint          NOT NULL,
            created_at       timestamp without time zone NOT NULL,
            date_completed   timestamp without time zone NOT NULL,
            id               uuid                   NOT NULL,
            original_size_in_bytes bigint           NOT NULL,
            priority         ds3.blob_store_task_priority NOT NULL,
            request_type     ds3.job_request_type   NOT NULL,
            user_id          uuid                   NOT NULL   REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,
            write_optimization ds3.write_optimization NOT NULL,
        
            PRIMARY KEY (id)
          );")
    execute("CREATE INDEX ds3_completed_job__bucket_id ON ds3.completed_job (bucket_id);")
    execute("CREATE INDEX ds3_completed_job__user_id ON ds3.completed_job (user_id);")


    execute("CREATE TABLE ds3.canceled_job (
          bucket_id        uuid                   NOT NULL   REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
          cached_size_in_bytes bigint             NOT NULL,
          chunk_client_processing_order_guarantee ds3.job_chunk_client_processing_order_guarantee NOT NULL,
          completed_size_in_bytes bigint          NOT NULL,
          created_at       timestamp without time zone NOT NULL,
          date_canceled    timestamp without time zone NOT NULL,
          id               uuid                   NOT NULL,
          original_size_in_bytes bigint           NOT NULL,
          priority         ds3.blob_store_task_priority NOT NULL,
          request_type     ds3.job_request_type   NOT NULL,
          user_id          uuid                   NOT NULL   REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,
          write_optimization ds3.write_optimization NOT NULL,
      
          PRIMARY KEY (id)
        );")
    execute("CREATE INDEX ds3_canceled_job__bucket_id ON ds3.canceled_job (bucket_id);")
    execute("CREATE INDEX ds3_canceled_job__user_id ON ds3.canceled_job (user_id);")
  end
    
  def down
    execute("DROP table ds3.completed_job;")
    execute("DROP table ds3.canceled_job;")
  end
end

