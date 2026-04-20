-- Add TapeDrive.minimumTaskPriority --
ALTER TABLE tape.tape_drive ADD COLUMN IF NOT EXISTS minimum_task_priority ds3.blob_store_task_priority;