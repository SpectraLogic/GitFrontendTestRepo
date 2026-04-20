-- Add TapeDrive.quiesced --
alter type shared.quiesced add value if not exists 'NO';
DO $$
BEGIN
alter table tape.tape_drive add column quiesced shared.quiesced;
update tape.tape_drive set quiesced='NO';
alter table tape.tape_drive alter column quiesced set not null;
END
$$;
