/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.aws;

import com.spectralogic.util.http.HttpHeaderType;
import com.spectralogic.util.lang.Validations;

public enum S3HeaderType implements HttpHeaderType
{
    ACCEPT( "Accept" ),
    ACCEPT_BYTE_RANGES( "Accept-Ranges" ),
    AMAZON_DATE( "x-amz-date" ),   
    AMAZON_REQUEST_ID( "x-amz-request-id" ),
    AUTHORIZATION( "Authorization" ),
    AUTH_DATE_GRACE( "x-amz-auth-date-grace" ),
    BYTE_RANGES( "Range" ),
    CONTENT_BYTE_RANGES( "Content-Range" ),
    CONTENT_LENGTH( "Content-Length" ),
    CONTENT_MD5( "Content-MD5" ),
    CONTENT_TYPE( "Content-Type" ),
    DATE( "Date" ),
    DISABLE_AUTHORIZATION_HEADER_VERIFICATION( "Disable-Authorization-Verification" ),
    ETAG( "ETag" ), 
    IMPERSONATE_USER( "Impersonate-User" ),
    INTERNAL_REQUEST_REQUIRING_AUTH_BYPASS( "Internal-Request" ),
    JOB_CHUNK_LOCK_HOLDER( "Job-Chunk-Lock-Holder" ),
    NAMING_CONVENTION( "Naming-Convention" ),
    OBJECT_CREATION_DATE( "Object-Creation-Date" ),
    PAGING_TOTAL_RESULT_COUNT( "Total-Result-Count" ),
    PAGING_TRUNCATED( "Page-Truncated" ),
    REPLICATION_SOURCE_IDENTIFIER( "Replication-Source-Identifier" ),
    SPECIFY_BY_ID( "Specify-By-Id" ),
    //A strict unversioned delete is one in which we want to do an unversioned delete, but we specify version ID's
    //anyway in order to assert what version we believe is the current latest. The request should fail if that version
    //is not the current latest. This is needed for ds3 replication.
    STRICT_UNVERSIONED_DELETE( "Strict-Unversioned-Delete" )
    ;
    
    
    private S3HeaderType( final String headerName )
    {
        m_headerName = headerName;
        Validations.verifyNotNull( "Header name", headerName );
    }
    
    
    public String getHttpHeaderName()
    {
        return m_headerName;
    }
    
    
    @Override
    public String toString()
    {
        return getHttpHeaderName();
    }
    
    
    public static boolean isCustomMetadata( final String headerName )
    {
        return headerName.startsWith( "x-amz-meta-" );
    }


    public static boolean isAmazonBuiltInHeader( final String headerName )
    {
        return headerName.startsWith( "x-amz-" );
    }
    
    
    public static S3HeaderType fromHeaderName( final String headerName )
    {
        Validations.verifyNotNull( "Header name", headerName );
        
        for ( final S3HeaderType header : S3HeaderType.class.getEnumConstants() )
        {
            if ( header.getHttpHeaderName().equalsIgnoreCase( headerName ) )
            {
                return header;
            }
        }
        return null;
    }
    
    
    private final String m_headerName;
}
