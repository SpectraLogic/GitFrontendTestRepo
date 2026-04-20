alter table ds3.data_path_backend add column default_verify_data_after_import ds3.blob_store_task_priority;
alter table ds3.data_path_backend add column default_verify_data_prior_to_import boolean;
update ds3.data_path_backend set default_verify_data_prior_to_import = true;
alter table ds3.data_path_backend alter column default_verify_data_prior_to_import set not null;
