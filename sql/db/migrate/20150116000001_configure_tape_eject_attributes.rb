class ConfigureTapeEjectAttributes < ActiveRecord::Migration[4.2]
  def up
    execute("alter table tape.tape add column eject_pending timestamp without time zone;")
    execute("alter table tape.tape add column eject_label varchar;")
    execute("alter table tape.tape add column eject_location varchar;")
    execute("alter table tape.tape add column eject_date timestamp without time zone;")
    
    execute("CREATE TYPE tape.import_export_configuration AS ENUM ('NOT_SUPPORTED');")
    execute("alter table tape.tape_partition add column import_export_configuration tape.import_export_configuration;")
    execute("update tape.tape_partition set import_export_configuration='NOT_SUPPORTED';")
    execute("alter table tape.tape_partition alter column import_export_configuration set not null;")
  end
    
  def down
    execute("alter table tape.tape drop column eject_pending;")
    execute("alter table tape.tape drop column eject_label;")
    execute("alter table tape.tape drop column eject_location;")
    execute("alter table tape.tape drop column eject_date;")

    execute("alter table tape.tape_partition drop column import_export_configuration;")
    execute("DROP TYPE tape.import_export_configuration;")
  end
end

