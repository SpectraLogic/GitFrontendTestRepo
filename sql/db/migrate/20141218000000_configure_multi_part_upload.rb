class ConfigureMultiPartUpload < ActiveRecord::Migration[4.2]
  def up
    execute("ALTER TABLE ds3.blob DROP COLUMN multi_part_upload_parent_blob_id")
    execute("CREATE TABLE ds3.multi_part_upload (
        id               uuid                   NOT NULL,
        placeholder_blob_id uuid                NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
    
        PRIMARY KEY (id)
      );
      CREATE INDEX ds3_multi_part_upload__placeholder_blob_id ON ds3.multi_part_upload (placeholder_blob_id);")
    execute("CREATE TABLE ds3.multi_part_upload_part (
          blob_id          uuid                   NOT NULL   REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE,
          id               uuid                   NOT NULL,
          multi_part_upload_id uuid               NOT NULL   REFERENCES ds3.multi_part_upload ON UPDATE CASCADE ON DELETE CASCADE,
          part_number      integer                NOT NULL,
      
          PRIMARY KEY (id), 
          UNIQUE (blob_id), 
          UNIQUE (multi_part_upload_id, part_number)
        );
      CREATE INDEX ds3_multi_part_upload_part__multi_part_upload_id ON ds3.multi_part_upload_part (multi_part_upload_id);
      ")
  end
    
  def down
    execute("DROP TABLE ds3.multi_part_upload_part")
    execute("DROP TABLE ds3.multi_part_upload")
    execute("ALTER TABLE ds3.blob ADD COLUMN multi_part_upload_parent_blob_id uuid NULL REFERENCES ds3.blob ON UPDATE CASCADE ON DELETE CASCADE")
  end
end

