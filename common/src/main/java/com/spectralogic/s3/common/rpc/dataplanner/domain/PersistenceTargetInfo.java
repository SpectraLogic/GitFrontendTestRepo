package com.spectralogic.s3.common.rpc.dataplanner.domain;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

import java.util.UUID;

public interface PersistenceTargetInfo  extends SimpleBeanSafeToProxy {

    String STORAGE_DOMAIN_IDS = "storageDomainIds";

    @Optional
    UUID[] getStorageDomainIds();

    PersistenceTargetInfo setStorageDomainIds(UUID[] value);


    String DS3_TARGET_IDS = "ds3TargetIds";

    @Optional
    UUID[] getDs3TargetIds();

    PersistenceTargetInfo setDs3TargetIds(UUID[] value);


    String S3_TARGET_IDS = "s3TargetIds";

    @Optional
    UUID[] getS3TargetIds();

    PersistenceTargetInfo setS3TargetIds(UUID[] value);


    String AZURE_TARGET_IDS = "azureTargetIds";

    @Optional
    UUID[] getAzureTargetIds();

    PersistenceTargetInfo setAzureTargetIds(UUID[] value);

}
