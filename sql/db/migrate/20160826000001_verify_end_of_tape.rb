class VerifyEndOfTape < ActiveRecord::Migration[4.2]
  def up
    execute("alter table tape.tape add column partially_verified_end_of_tape timestamp without time zone;")
    execute("alter table ds3.data_path_backend add column partially_verify_last_percent_of_tapes integer;")
  end
    
  def down
    raise(ActiveRecord::IrreversibleMigration)
  end
end

