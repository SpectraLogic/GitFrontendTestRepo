class ImportTapeDirective < ActiveRecord::Migration[4.2]
  def up
    execute("CREATE TABLE tape.import_tape_directive (
      id               uuid                   NOT NULL,
      tape_id          uuid                   NOT NULL   REFERENCES tape.tape ON UPDATE CASCADE ON DELETE CASCADE,
      user_id          uuid                   NOT NULL   REFERENCES ds3.user ON UPDATE CASCADE ON DELETE CASCADE,
  
      PRIMARY KEY (id), 
      UNIQUE (tape_id)
    );")
    execute("CREATE INDEX tape_import_tape_directive__user_id ON tape.import_tape_directive (user_id);")
  end

  def down
    execute("DROP table tape.import_tape_directive;")
  end
end

