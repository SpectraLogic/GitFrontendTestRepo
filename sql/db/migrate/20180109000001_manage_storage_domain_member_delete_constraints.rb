require "spectra_support/cmd"

class ManageStorageDomainMemberDeleteConstraints < ActiveRecord::Migration[4.2]
  def up
    execute('ALTER TABLE ds3.storage_domain_member DROP CONSTRAINT storage_domain_member_tape_partition_id_fkey, ADD CONSTRAINT storage_domain_member_tape_partition_id_fkey FOREIGN KEY (tape_partition_id) REFERENCES tape.tape_partition (id) ON UPDATE CASCADE ON DELETE CASCADE')
    execute('ALTER TABLE ds3.storage_domain_member DROP CONSTRAINT storage_domain_member_pool_partition_id_fkey, ADD CONSTRAINT storage_domain_member_pool_partition_id_fkey FOREIGN KEY (pool_partition_id) REFERENCES pool.pool_partition (id) ON UPDATE CASCADE ON DELETE CASCADE')
    execute('ALTER TABLE tape.tape DROP CONSTRAINT tape_storage_domain_member_id_fkey, ADD CONSTRAINT tape_storage_domain_member_id_fkey FOREIGN KEY (storage_domain_member_id) REFERENCES ds3.storage_domain_member (id) ON UPDATE CASCADE ON DELETE RESTRICT' )
    execute('ALTER TABLE pool.pool DROP CONSTRAINT pool_storage_domain_member_id_fkey, ADD CONSTRAINT pool_storage_domain_member_id_fkey FOREIGN KEY (storage_domain_member_id) REFERENCES ds3.storage_domain_member (id) ON UPDATE CASCADE ON DELETE RESTRICT' )
  end

  def down
    execute('ALTER TABLE ds3.storage_domain_member DROP CONSTRAINT storage_domain_member_tape_partition_id_fkey, ADD CONSTRAINT storage_domain_member_tape_partition_id_fkey FOREIGN KEY (tape_partition_id) REFERENCES tape.tape_partition (id) ON UPDATE CASCADE')
    execute('ALTER TABLE ds3.storage_domain_member DROP CONSTRAINT storage_domain_member_pool_partition_id_fkey, ADD CONSTRAINT storage_domain_member_pool_partition_id_fkey FOREIGN KEY (pool_partition_id) REFERENCES pool.pool_partition (id) ON UPDATE CASCADE')
    execute('ALTER TABLE tape.tape DROP CONSTRAINT tape_storage_domain_member_id_fkey, ADD CONSTRAINT tape_storage_domain_member_id_fkey FOREIGN KEY (storage_domain_member_id) REFERENCES ds3.storage_domain_member (id) ON UPDATE CASCADE ON DELETE SET NULL' )
    execute('ALTER TABLE pool.pool DROP CONSTRAINT pool_storage_domain_member_id_fkey, ADD CONSTRAINT pool_storage_domain_member_id_fkey FOREIGN KEY (storage_domain_member_id) REFERENCES ds3.storage_domain_member (id) ON UPDATE CASCADE ON DELETE SET NULL' )
  end
end