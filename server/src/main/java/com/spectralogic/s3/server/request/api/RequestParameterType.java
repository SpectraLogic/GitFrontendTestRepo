/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.request.api;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.lang.Validations;

/**
 * A Request parameter is a query parameter in the URI path of the HTTP request.
 */
public enum RequestParameterType
{
    BLOB( UUID.class ),
    BLOBS( void.class ),
    BUCKET( String.class ),
    BUCKET_ID( String.class ),
    CACHED_ONLY( void.class ),
    CHARACTERIZE( void.class ),
    CLOSE_AGGREGATING_JOB ( void.class ),
    CONVERT_TO_DS3_TARGET( UUID.class ),
    DELETE( void.class ),
    END_DATE( long.class ),
    FORCE( void.class ),
    FULL_DETAILS( void.class ),
    IGNORE_NAMING_CONFLICTS( void.class ),
    INCLUDE_PHYSICAL_PLACEMENT( void.class ),
    JOB( UUID.class ),
    JOB_CHUNK( UUID.class ),
    LAST_PAGE( void.class ),
    MAX_PARTS( int.class ),
    MAX_UPLOAD_SIZE( long.class ),
    MIN_SEQUENCE_NUMBER( long.class ),
    OFFSET( long.class ),
    OPERATION( RestOperationType.class ),
    PAGE_LENGTH( int.class ),
    PAGE_OFFSET( int.class ),
    PAGE_START_MARKER( UUID.class ),
    PARTITION( String.class ),
    PART_NUMBER( int.class ),
    PART_NUMBER_MARKER( int.class ),
    PREFERRED_NUMBER_OF_CHUNKS( int.class ),
    PRE_ALLOCATE_JOB_SPACE( void.class ),
    RECLAIM( void.class ),
    RECURSIVE( void.class ),
    REPLICATE( void.class ),
    SIZE( long.class ),
    SKIP_CLEAN( void.class ),
	SORT_BY( String.class ),
	START_DATE( long.class ),
    STORAGE_DOMAIN( String.class ),
    SUMMARY( void.class ),
    TAPE_ID(UUID.class),
    TASK_PRIORITY( BlobStoreTaskPriority.class ),
    UPLOADS( void.class ),
    UPLOAD_ID( UUID.class ),
    VERSIONS( void.class ),
    VERSION_ID( UUID.class )
    ;
    
    
    /**
     * @param valueType the type of the value, or void.class if the presence of the request parameter is the only thing
     * that matters and the value supplied is meaningless
     */
    RequestParameterType( final Class< ? > valueType )
    {
        m_valueType = valueType;
        Validations.verifyNotNull( "Value type", m_valueType );
    }
    
    
    public Class< ? > getValueType()
    {
        return m_valueType;
    }
    
    
    private final Class< ? > m_valueType;
}
