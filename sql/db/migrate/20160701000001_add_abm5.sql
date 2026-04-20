alter table ds3.job add column truncated_due_to_timeout boolean;
update ds3.job set truncated_due_to_timeout = false;
alter table ds3.job alter column truncated_due_to_timeout set not null;
