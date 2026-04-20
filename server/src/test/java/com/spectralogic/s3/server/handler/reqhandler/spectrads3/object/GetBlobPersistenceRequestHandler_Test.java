/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionState;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.platform.spectrads3.BlobIdsSpecification;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistence;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.marshal.JsonMarshaler;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class GetBlobPersistenceRequestHandler_Test 
{
    @Test
    public void testGetWithInvalidPayloadNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final byte[] requestPayload = ( "oops" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.BLOB_PERSISTENCE );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testGetDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final S3Object o1 = mockDaoDriver.createObjectStub( null, "o1", 10 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObjectStub( null, "o2", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Object o3 = mockDaoDriver.createObjectStub( null, "o3", 10 );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.createObjectStub( null, "o4", 10 );
        
        mockDaoDriver.simulateObjectUploadCompletion( o1.getId() );
        mockDaoDriver.simulateObjectUploadCompletion( o2.getId() );
        
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, null );
        final TapePartition tp2 = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.updateBean( tp1.setState( TapePartitionState.ONLINE ), TapePartition.STATE );
        mockDaoDriver.updateBean( tp2.setState( TapePartitionState.OFFLINE ), TapePartition.STATE );
        
        final Tape t1 = mockDaoDriver.createTape( tp1.getId(), TapeState.NORMAL );
        final Tape t2 = mockDaoDriver.createTape( tp1.getId(), TapeState.EJECTED );
        final Tape t3 = mockDaoDriver.createTape( tp2.getId(), TapeState.NORMAL );
        final Pool p1 = mockDaoDriver.createPool( PoolState.LOST );
        final Pool p2 = mockDaoDriver.createPool( PoolState.NORMAL );
        
        mockDaoDriver.putBlobOnTape( t1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( t2.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( t3.getId(), b2.getId() );
        
        mockDaoDriver.putBlobOnPool( p1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnPool( p2.getId(), b2.getId() );
        
        final UUID absentBlobId = UUID.randomUUID();
        final BlobIdsSpecification blobIds = BeanFactory.newBean( BlobIdsSpecification.class );
        blobIds.setBlobIds( new UUID [] { b1.getId(), b2.getId(), b3.getId(), absentBlobId } );
        blobIds.setJobId( UUID.randomUUID() );
        final byte[] requestPayload = blobIds.toJson(
                NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE ).getBytes( 
                        Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.BLOB_PERSISTENCE );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final Set< UUID > ids = new HashSet<>();
        final BlobPersistenceContainer response = JsonMarshaler.unmarshal(
                BlobPersistenceContainer.class, 
                driver.getResponseToClientAsString() );
        assertFalse(response.isJobExistant(), "Shoulda reported no job.");
        for ( final BlobPersistence blob : response.getBlobs() )
        {
            ids.add( blob.getId() );
            if ( b3.getId().equals( blob.getId() ) )
            {
                assertNull(blob.getChecksum(), "Shoulda reported no checksum for blob 3.");
                assertNull(blob.getChecksumType(), "Shoulda reported no checksum for blob 3.");
            }
            else
            {
                assertNotNull(blob.getChecksum(), "Shoulda reported no checksum for blobs 1 and 2.");
                assertNotNull(blob.getChecksumType(), "Shoulda reported no checksum for blobs 1 and 2.");
                final Object expected1 = b1.getId().equals( blob.getId() );
                assertEquals(expected1, blob.isAvailableOnTapeNow(), "Shoulda reported on tape for blob 1 only.");
                final Object expected = b2.getId().equals( blob.getId() );
                assertEquals(expected, blob.isAvailableOnPoolNow(), "Shoulda reported on pool for blob 2 only.");
            }
        }
        assertEquals(3,  ids.size(), "Shoulda reported the 3 blobs that are defined.");
    }
}
