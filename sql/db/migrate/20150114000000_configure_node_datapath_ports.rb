class ConfigureNodeDatapathPorts < ActiveRecord::Migration[4.2]
  def up
    execute("alter table ds3.node add column data_path_http_port int;")
    execute("alter table ds3.node add column data_path_https_port int;")
  end
    
  def down
    execute("alter table ds3.node drop column data_path_http_port;")
    execute("alter table ds3.node drop column data_path_https_port;")
  end
end

