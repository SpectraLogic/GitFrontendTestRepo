alter table ds3.job add column replicating boolean;
update ds3.job set replicating = false;
alter table ds3.job alter column replicating set not null;
