-- Add Target.S3MultipartUpload --
CREATE TABLE target.s3_multipart_upload (
  object_id          uuid                NOT NULL REFERENCES ds3.s3_object ON UPDATE CASCADE ON DELETE CASCADE,
  upload_id          varchar             NOT NULL,
  id                 uuid                NOT NULL,
  PRIMARY KEY (id),
  UNIQUE (object_id),
  UNIQUE (upload_id)
);


-- Add Target.namingMode --
CREATE TYPE ds3.cloud_naming_mode AS ENUM ();
alter type ds3.cloud_naming_mode add value if not exists 'BLACK_PEARL';
alter table target.s3_target add column naming_mode ds3.cloud_naming_mode;
alter table target.azure_target add column naming_mode ds3.cloud_naming_mode;
update target.s3_target set naming_mode='BLACK_PEARL';
update target.azure_target set naming_mode='BLACK_PEARL';
alter table target.s3_target alter column naming_mode set not null;
alter table target.azure_target alter column naming_mode set not null;