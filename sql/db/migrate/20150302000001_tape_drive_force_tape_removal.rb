class TapeDriveForceTapeRemoval < ActiveRecord::Migration[4.2]
  def up
    execute("alter table tape.tape_drive add column force_tape_removal boolean;")
    execute("update tape.tape_drive set force_tape_removal=false;")
    execute("alter table tape.tape_drive alter column force_tape_removal set not null;")
  end

  def down
    execute("alter table tape.tape_drive drop column force_tape_removal;")
  end
end

