class VerifyThenEject < ActiveRecord::Migration[4.2]
  def up
    execute("alter table tape.tape add column verify_pending ds3.blob_store_task_priority;")
    execute("alter table ds3.storage_domain add column verify_prior_to_auto_eject ds3.blob_store_task_priority;")
  end
    
  def down
    execute("alter table tape.tape drop column verify_pending;")
    execute("alter table ds3.storage_domain drop column verify_prior_to_auto_eject;")
  end
end

