/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.request.rest;

import com.spectralogic.util.lang.Validations;

/**
 * All the domains where we support RESTful request handling.
 */
public enum RestDomainType
{
    /*
     * Keep entries in this list singular and alphabetically sorted.
     */
    ABM_CONFIG( RestResourceType.SINGLETON ),
    ACTIVE_JOB( RestResourceType.NON_SINGLETON ),
    AZURE_DATA_REPLICATION_RULE( RestResourceType.NON_SINGLETON ),
    AZURE_TARGET( RestResourceType.NON_SINGLETON ),
    AZURE_TARGET_BUCKET_NAME( RestResourceType.NON_SINGLETON ),
    AZURE_TARGET_FAILURE( RestResourceType.NON_SINGLETON ),
    AZURE_TARGET_FAILURE_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    AZURE_TARGET_READ_PREFERENCE( RestResourceType.NON_SINGLETON ),
    BEANS_RETRIEVER( RestResourceType.NON_SINGLETON ),
    BLOB_PERSISTENCE( RestResourceType.NON_SINGLETON ),
    BLOB_STORE_TASK( RestResourceType.NON_SINGLETON ),
    BUCKET( RestResourceType.NON_SINGLETON ),
    BUCKET_ACL( RestResourceType.NON_SINGLETON ),
    BUCKET_CAPACITY_SUMMARY( RestResourceType.NON_SINGLETON ),
    BUCKET_CHANGES_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    BUCKET_HISTORY( RestResourceType.NON_SINGLETON ),
    CACHE_FILESYSTEM( RestResourceType.NON_SINGLETON ),
    CACHE_STATE( RestResourceType.NON_SINGLETON ),
    CACHE_THROTTLE_RULE( RestResourceType.NON_SINGLETON ),
    CANCELED_JOB( RestResourceType.NON_SINGLETON ),
    CAPACITY_SUMMARY( RestResourceType.SINGLETON ),
    COMPLETED_JOB( RestResourceType.NON_SINGLETON ),
    DATA_PATH( RestResourceType.SINGLETON ),
    DATA_PATH_BACKEND( RestResourceType.SINGLETON ),
    DATA_PERSISTENCE_RULE( RestResourceType.NON_SINGLETON ),
    DATA_POLICY( RestResourceType.NON_SINGLETON ),
    DATA_POLICY_ACL( RestResourceType.NON_SINGLETON ),
    DEGRADED_AZURE_DATA_REPLICATION_RULE( RestResourceType.NON_SINGLETON ),
    DEGRADED_BLOB( RestResourceType.NON_SINGLETON ),
    DEGRADED_BUCKET( RestResourceType.NON_SINGLETON ),
    DEGRADED_DATA_PERSISTENCE_RULE( RestResourceType.NON_SINGLETON ),
    DEGRADED_DS3_DATA_REPLICATION_RULE( RestResourceType.NON_SINGLETON ),
    DEGRADED_S3_DATA_REPLICATION_RULE( RestResourceType.NON_SINGLETON ),
    DS3_DATA_REPLICATION_RULE( RestResourceType.NON_SINGLETON ),
    DS3_TARGET( RestResourceType.NON_SINGLETON ),
    DS3_TARGET_DATA_POLICIES( RestResourceType.NON_SINGLETON ),
    DS3_TARGET_FAILURE( RestResourceType.NON_SINGLETON ),
    DS3_TARGET_FAILURE_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    DS3_TARGET_READ_PREFERENCE( RestResourceType.NON_SINGLETON ),
    FEATURE_KEY( RestResourceType.NON_SINGLETON ),
    FOLDER( RestResourceType.NON_SINGLETON ),
    GENERIC_DAO_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    GROUP( RestResourceType.NON_SINGLETON ),
    GROUP_MEMBER( RestResourceType.NON_SINGLETON ),
    HEAP_DUMP( RestResourceType.NON_SINGLETON ),
    INSTANCE_IDENTIFIER( RestResourceType.SINGLETON ),
    JOB( RestResourceType.NON_SINGLETON ),
    JOB_CHUNK( RestResourceType.NON_SINGLETON ),
    JOB_CHUNK_DAO( RestResourceType.NON_SINGLETON ),
    JOB_COMPLETED_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    JOB_CREATED_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    JOB_CREATION_FAILED( RestResourceType.NON_SINGLETON ),
    JOB_CREATION_FAILED_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    NODE( RestResourceType.NON_SINGLETON ),
    OBJECT( RestResourceType.NON_SINGLETON ),
    OBJECT_CACHED_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    OBJECT_LOST_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    OBJECT_PERSISTED_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    POOL( RestResourceType.NON_SINGLETON ),
    POOL_ENVIRONMENT( RestResourceType.SINGLETON ),
    POOL_FAILURE( RestResourceType.NON_SINGLETON ),
    POOL_FAILURE_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    POOL_PARTITION( RestResourceType.NON_SINGLETON ),
    REQUEST_HANDLER( RestResourceType.SINGLETON ),
    REQUEST_HANDLER_CONTRACT( RestResourceType.SINGLETON ),
    S3_DATA_REPLICATION_RULE( RestResourceType.NON_SINGLETON ),
    S3_TARGET( RestResourceType.NON_SINGLETON ),
    S3_TARGET_BUCKET_NAME( RestResourceType.NON_SINGLETON ),
    S3_TARGET_FAILURE( RestResourceType.NON_SINGLETON ),
    S3_TARGET_FAILURE_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    S3_TARGET_READ_PREFERENCE( RestResourceType.NON_SINGLETON ),
    STORAGE_DOMAIN( RestResourceType.NON_SINGLETON ),
    STORAGE_DOMAIN_FAILURE( RestResourceType.NON_SINGLETON ),
    STORAGE_DOMAIN_FAILURE_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    STORAGE_DOMAIN_MEMBER( RestResourceType.NON_SINGLETON ),
    SUSPECT_BLOB_AZURE_TARGET( RestResourceType.NON_SINGLETON ),
    SUSPECT_BLOB_DS3_TARGET( RestResourceType.NON_SINGLETON ),
    SUSPECT_BLOB_POOL( RestResourceType.NON_SINGLETON ),
    SUSPECT_BLOB_S3_TARGET( RestResourceType.NON_SINGLETON ),
    SUSPECT_BLOB_TAPE( RestResourceType.NON_SINGLETON ),
    SUSPECT_BUCKET( RestResourceType.NON_SINGLETON ),
    SUSPECT_OBJECT( RestResourceType.NON_SINGLETON ),
    SYSTEM_FAILURE( RestResourceType.NON_SINGLETON ),
    SYSTEM_FAILURE_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    SYSTEM_HEALTH( RestResourceType.SINGLETON ),
    SYSTEM_INFORMATION( RestResourceType.SINGLETON ),
    TAPE( RestResourceType.NON_SINGLETON ),
    TAPE_BUCKET( RestResourceType.NON_SINGLETON ),
    TAPE_DENSITY_DIRECTIVE( RestResourceType.NON_SINGLETON ),
    TAPE_DRIVE( RestResourceType.NON_SINGLETON ),
    TAPE_ENVIRONMENT( RestResourceType.SINGLETON ),
    TAPE_FAILURE( RestResourceType.NON_SINGLETON ),
    TAPE_FAILURE_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    TAPE_LIBRARY( RestResourceType.NON_SINGLETON ),
    TAPE_PARTITION( RestResourceType.NON_SINGLETON ),
    TAPE_PARTITION_FAILURE( RestResourceType.NON_SINGLETON ),
    TAPE_PARTITION_FAILURE_NOTIFICATION_REGISTRATION( RestResourceType.NON_SINGLETON ),
    TARGET_ENVIRONMENT( RestResourceType.SINGLETON ),
    USER( RestResourceType.NON_SINGLETON ),
    USER_INTERNAL( RestResourceType.NON_SINGLETON ),
    ;
    
    private RestDomainType( final RestResourceType resourceType )
    {
        Validations.verifyNotNull( "Resource type", resourceType );
        m_resourceType = resourceType;
    }
    
    
    public RestResourceType getResourceType()
    {
        return m_resourceType;
    }
    
    
    private final RestResourceType m_resourceType;
}
