-- com.spectralogic.s3.common.dao.domain.target.S3Target --
ALTER TABLE target.s3_target DROP CONSTRAINT s3_target_data_path_end_point;
ALTER TABLE target.s3_target DROP CONSTRAINT s3_target_access_key;
ALTER TABLE target.s3_target ADD CONSTRAINT s3_target_data_path_end_point_access_key UNIQUE (data_path_end_point, access_key);