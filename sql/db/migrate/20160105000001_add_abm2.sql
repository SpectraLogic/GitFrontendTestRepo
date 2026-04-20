-- com.spectralogic.s3.common.dao.domain.ds3.UnavailableMediaUsagePolicy --
CREATE TYPE ds3.unavailable_media_usage_policy AS ENUM ('DISCOURAGED');

-- com.spectralogic.s3.common.dao.domain.ds3.AutoInspectMode --
CREATE TYPE ds3.auto_inspect_mode AS ENUM ('DEFAULT');

alter table ds3.data_path_backend drop column allow_new_jobs_to_use_unavailable_tape_partitions;
alter table ds3.data_path_backend drop column allow_new_jobs_to_use_unavailable_pools;
alter table ds3.data_path_backend drop column auto_inspect_on_startup;
    
alter table ds3.data_path_backend add column unavailable_media_policy ds3.unavailable_media_usage_policy;
update ds3.data_path_backend set unavailable_media_policy='DISCOURAGED';
alter table ds3.data_path_backend alter column unavailable_media_policy set not null;

alter table ds3.data_path_backend add column auto_inspect ds3.auto_inspect_mode;
update ds3.data_path_backend set auto_inspect='DEFAULT';
alter table ds3.data_path_backend alter column auto_inspect set not null;
    
alter table ds3.job add column name varchar;
update ds3.job set name='Untitled';
alter table ds3.job alter column name set not null;
    
alter table ds3.completed_job add column name varchar;
update ds3.completed_job set name='Untitled';
alter table ds3.completed_job alter column name set not null;
    
alter table ds3.canceled_job add column name varchar;
update ds3.canceled_job set name='Untitled';
alter table ds3.canceled_job alter column name set not null;

alter table ds3.job_chunk_persistence_target add column done boolean;
update ds3.job_chunk_persistence_target set done=false;
alter table ds3.job_chunk_persistence_target alter column done set not null;

alter table ds3.job drop column reshapable;
alter table ds3.job add column aggregating boolean;
update ds3.job set aggregating=false;
alter table ds3.job alter column aggregating set not null;