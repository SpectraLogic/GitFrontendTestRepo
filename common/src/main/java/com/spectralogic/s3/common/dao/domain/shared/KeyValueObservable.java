/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.shared;

public interface KeyValueObservable< T >
{
    String KEY = "key";
    
    String getKey();
    
    T setKey( final String value );
    
    
    String VALUE = "value";
    
    String getValue();
    
    T setValue( final String value );
    
    
    String SPECTRA_KEY_NAMESPACE = "x-spectra-";
    String AMZ_META_PREFIX = "x-amz-meta-";
    //UNPROTECTED_SPECTRA_KEY_NAMESPACE keys will not be blocked in metadata created by clients 
    String UNPROTECTED_SPECTRA_KEY_NAMESPACE = AMZ_META_PREFIX + "o-spectra-";
    String CREATION_DATE = SPECTRA_KEY_NAMESPACE + "creation-date";
    String TOTAL_BLOB_COUNT = SPECTRA_KEY_NAMESPACE + "total-blob-count";
    String BACKUP_INSTANCE_ID = UNPROTECTED_SPECTRA_KEY_NAMESPACE + "backup-instance-id";
    String BACKUP_START_DATE = UNPROTECTED_SPECTRA_KEY_NAMESPACE + "backup-start-date";
}
