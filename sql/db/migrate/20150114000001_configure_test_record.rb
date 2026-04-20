class ConfigureTestRecord < ActiveRecord::Migration[4.2]
  def up
    execute("CREATE TABLE framework.test_record (
      boolean_value     boolean                NULL,
      double_value      double precision       NULL,
      id                uuid                   NOT NULL,
      key               varchar                NOT NULL,
      long_value        bigint                 NULL,
      string_value      varchar                NULL,
  
      PRIMARY KEY (id),
      UNIQUE (key)
    );")
  end
    
  def down
    execute("drop table framework.test_record;")
  end
end

