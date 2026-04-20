-- com.spectralogic.s3.common.dao.domain.target.S3Target --
ALTER TABLE target.s3_target ADD CONSTRAINT s3_target_data_path_end_point UNIQUE (data_path_end_point);
ALTER TABLE target.s3_target ADD CONSTRAINT s3_target_access_key UNIQUE (access_key);

-- com.spectralogic.s3.common.dao.domain.target.AzureTarget --
ALTER TABLE target.azure_target ADD CONSTRAINT azure_target_account_name UNIQUE (account_name);