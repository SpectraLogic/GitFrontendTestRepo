/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;


import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.security.ChecksumType;
import org.junit.jupiter.api.Test;

public class HeadObjectRequestHandler_Test 
{
    @Test
    public void testHeadObjectReturns200WhenObjectExists()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        mockDaoDriver.createObject( bucketId, "test_object_name" );
    
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.HEAD,
                "test_bucket_name/test_object_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final Map< String, String > requiredHeaders = new HashMap<>();
        requiredHeaders.put( S3HeaderType.CONTENT_LENGTH.getHttpHeaderName(), "10" );
        driver.assertResponseToClientHasHeaders( requiredHeaders );
    }
    
    
    @Test
    public void testHeadObjectReturns404WhenObjectDoesNotExist()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
    
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.HEAD,
                "test_bucket_name/test_object_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
    
    
    @Test
    public void testHeadObjectReturnsAllBlobChecksumInformation()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" )
                                           .getId();
    
        for ( ChecksumType checksumType : ChecksumType.values() )
        {
            final S3Object object = mockDaoDriver.createObject( bucketId, checksumType.toString().toLowerCase(), -1 );
            final List< Blob > blobs = mockDaoDriver.createBlobs( object.getId(), 5, 42 );
            blobs.forEach( x -> mockDaoDriver.updateBean( x.setChecksum( "checksum_of_blob_at_offset_" + x.getByteOffset() )
                                                           .setChecksumType( checksumType ), Blob.CHECKSUM,
                    Blob.CHECKSUM_TYPE ) );
        
            final MockHttpRequestDriver driver =
                    new MockHttpRequestDriver( support, true, new MockInternalRequestAuthorizationStrategy(),
                            RequestType.HEAD, "test_bucket_name/" + checksumType.toString().toLowerCase() );
            driver.run();
            driver.assertHttpResponseCodeEquals( 200 );
        
            final Map< String, String > requiredHeaders = new HashMap<>();
            requiredHeaders.put( "ds3-blob-checksum-type", checksumType.toString() );
            requiredHeaders.put( "ds3-blob-checksum-offset-0", "checksum_of_blob_at_offset_0" );
            requiredHeaders.put( "ds3-blob-checksum-offset-42", "checksum_of_blob_at_offset_42" );
            requiredHeaders.put( "ds3-blob-checksum-offset-84", "checksum_of_blob_at_offset_84" );
            requiredHeaders.put( "ds3-blob-checksum-offset-126", "checksum_of_blob_at_offset_126" );
            requiredHeaders.put( "ds3-blob-checksum-offset-168", "checksum_of_blob_at_offset_168" );
            driver.assertResponseToClientHasHeaders( requiredHeaders );
        }
    }
    
    
    @Test
    public void testHeadObjectReturnsMetadataWhenMetadataExists()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final Map< String, String > propertiesMapping = new HashMap<>();
        propertiesMapping.put( "x-amz-meta-test", "this value tests custom metadata" );
        propertiesMapping.put( "x-amz-meta-another", "more than one metadata field can be set" );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final UUID objectId = mockDaoDriver.createObject( bucketId, "test_object_name" ).getId();
        mockDaoDriver.createObjectProperties( objectId, propertiesMapping );
    
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.HEAD,
                "test_bucket_name/test_object_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientHasHeaders( propertiesMapping );
    }
}
