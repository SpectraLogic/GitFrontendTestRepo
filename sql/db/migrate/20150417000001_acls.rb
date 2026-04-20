class Acls < ActiveRecord::Migration[4.2]
  def up
    # Delete legacy ACL dao that we NEVER actually used
    execute("drop table ds3.acl;")
    execute("drop type ds3.acl_permission;")
    
    # Wipe out built-in groups (we'll re-create them below)
    execute("drop table ds3.group_member;")
    execute("drop table ds3.group;")
    
    # Create correct ACL dao
    execute("CREATE TABLE ds3.group (
              built_in         boolean                NOT NULL,
              id               uuid                   NOT NULL,
              name             varchar                NOT NULL,
          
              PRIMARY KEY (id), 
              UNIQUE (name)
            );")
    execute("CREATE TABLE ds3.group_member (
              group_id         uuid                   NOT NULL   REFERENCES ds3.group ON UPDATE CASCADE ON DELETE CASCADE,
              id               uuid                   NOT NULL,
              member_group_id  uuid                   NULL       REFERENCES ds3.group ON UPDATE CASCADE ON DELETE CASCADE,
              member_user_id   uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,
          
              PRIMARY KEY (id), 
              UNIQUE (group_id, member_group_id), 
              UNIQUE (group_id, member_user_id)
            );
          CREATE INDEX ON ds3.group_member (group_id);
          CREATE INDEX ON ds3.group_member (member_group_id);
          CREATE INDEX ON ds3.group_member (member_user_id);")
    execute("CREATE TYPE ds3.bucket_acl_permission AS ENUM ();")
    execute("CREATE TABLE ds3.bucket_acl (
              bucket_id        uuid                   NULL       REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
              group_id         uuid                   NULL       REFERENCES ds3.group ON UPDATE CASCADE ON DELETE CASCADE,
              id               uuid                   NOT NULL,
              permission       ds3.bucket_acl_permission NOT NULL,
              user_id          uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,
          
              PRIMARY KEY (id), 
              UNIQUE (bucket_id, group_id, permission), 
              UNIQUE (bucket_id, user_id, permission)
            );
          CREATE INDEX ON ds3.bucket_acl (bucket_id);
          CREATE INDEX ON ds3.bucket_acl (group_id);
          CREATE INDEX ON ds3.bucket_acl (user_id);")
  end

  def down
    # Re-create legacy ACL dao that we NEVER actually used
    execute("CREATE TYPE ds3.acl_permission AS ENUM ();")
    execute("CREATE TABLE ds3.acl (
              bucket_id         uuid                   NULL       REFERENCES ds3.bucket ON UPDATE CASCADE ON DELETE CASCADE,
              group_id          uuid                   NULL       REFERENCES ds3.group ON UPDATE CASCADE ON DELETE CASCADE,
              id                uuid                   NOT NULL,
              object_id         uuid                   NULL       REFERENCES ds3.s3_object ON UPDATE CASCADE ON DELETE CASCADE,
              permission        ds3.acl_permission     NOT NULL,
              user_id           uuid                   NULL       REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,
          
              PRIMARY KEY (id)
            );")
    execute("CREATE INDEX ds3_acl__bucket_id on ds3.acl (bucket_id);")
    execute("CREATE INDEX ds3_acl__group_id on ds3.acl (group_id);")
    execute("CREATE INDEX ds3_acl__object_id on ds3.acl (object_id);")
    execute("CREATE INDEX ds3_acl__user_id on ds3.acl (user_id);")
  end
end

