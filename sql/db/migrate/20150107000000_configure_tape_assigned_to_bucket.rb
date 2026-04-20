class ConfigureTapeAssignedToBucket < ActiveRecord::Migration[4.2]
  def up
    execute("alter table tape.tape add column assigned_to_bucket boolean;")
    execute("update tape.tape set assigned_to_bucket=true where not bucket_id is null;")
    execute("update tape.tape set assigned_to_bucket=false where bucket_id is null;")
    execute("alter table tape.tape alter column assigned_to_bucket set not null;")
    execute("alter table tape.tape drop constraint tape_bucket_id_fkey;")
    execute("alter table tape.tape add constraint tape_bucket_id_fkey foreign key (bucket_id) 
            references ds3.bucket(id) on update cascade on delete set null;")
  end
    
  def down
    execute("alter table tape.tape drop column assigned_to_bucket;")
    execute("alter table tape.tape drop constraint tape_bucket_id_fkey;")
    execute("alter table tape.tape add constraint tape_bucket_id_fkey foreign key (bucket_id) 
            references ds3.bucket(id) on update cascade;")
  end
end

