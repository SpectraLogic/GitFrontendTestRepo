-- Add TapeDrive.mfgSerialNumber --
alter table tape.tape_drive add column mfg_serial_number varchar;

-- Add TapeDrive.cleaningRequired --
alter table tape.tape_drive add column cleaning_required boolean;
update tape.tape_drive set cleaning_required=false;
alter table tape.tape_drive alter column cleaning_required set not null;

-- Add Job.deadJobCleanupAllowed --
alter table ds3.job add column dead_job_cleanup_allowed boolean;
update ds3.job set dead_job_cleanup_allowed=true;
alter table ds3.job alter column dead_job_cleanup_allowed set not null;