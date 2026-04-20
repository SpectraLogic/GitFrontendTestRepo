-- Add TapeDrive.reservedTaskType --
CREATE type shared.reserved_task_type AS ENUM ();
ALTER type shared.reserved_task_type ADD VALUE IF NOT EXISTS 'ANY';
ALTER TABLE tape.tape_drive ADD COLUMN IF NOT EXISTS reserved_task_type shared.reserved_task_type;
UPDATE tape.tape_drive SET reserved_task_type='ANY';
ALTER TABLE tape.tape_drive ALTER COLUMN reserved_task_type SET NOT NULL;

-- Add TapePartition.minimumReadReservedDrives --
ALTER TABLE tape.tape_partition ADD COLUMN IF NOT EXISTS minimum_read_reserved_drives INT;
UPDATE tape.tape_partition SET minimum_read_reserved_drives=0;
ALTER TABLE tape.tape_partition ALTER COLUMN minimum_read_reserved_drives SET NOT NULL;

-- Add TapePartition.minimumWriteReservedDrives --
ALTER TABLE tape.tape_partition ADD COLUMN IF NOT EXISTS minimum_write_reserved_drives INT;
UPDATE tape.tape_partition SET minimum_write_reserved_drives=0;
ALTER TABLE tape.tape_partition ALTER COLUMN minimum_write_reserved_drives SET NOT NULL;
