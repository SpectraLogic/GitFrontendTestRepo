/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend.dataorder;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;



import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.AzureTargetReadPreference;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreferenceType;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class PublicCloudBlobSupport_Test
{

    @Test
    public void testConstructorInvalidInputNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );

        new PublicCloudBlobSupport<>(
                AzureTarget.class,
                BlobAzureTarget.class,
                SuspectBlobAzureTarget.class,
                AzureTargetReadPreference.class,
                CollectionFactory.toSet( blob.getId() ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudBlobSupport<>(
                        null,
                        BlobAzureTarget.class,
                        SuspectBlobAzureTarget.class,
                        AzureTargetReadPreference.class,
                        CollectionFactory.toSet( blob.getId() ),
                        dbSupport.getServiceManager() );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudBlobSupport<>(
                        AzureTarget.class,
                        null,
                        SuspectBlobAzureTarget.class,
                        AzureTargetReadPreference.class,
                        CollectionFactory.toSet( blob.getId() ),
                        dbSupport.getServiceManager() );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudBlobSupport<>(
                        AzureTarget.class,
                        BlobAzureTarget.class,
                        null,
                        AzureTargetReadPreference.class,
                        CollectionFactory.toSet( blob.getId() ),
                        dbSupport.getServiceManager() );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudBlobSupport<>(
                        AzureTarget.class,
                        BlobAzureTarget.class,
                        SuspectBlobAzureTarget.class,
                        null,
                        CollectionFactory.toSet( blob.getId() ),
                        dbSupport.getServiceManager() );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudBlobSupport<>(
                        AzureTarget.class,
                        BlobAzureTarget.class,
                        SuspectBlobAzureTarget.class,
                        AzureTargetReadPreference.class,
                        null,
                        dbSupport.getServiceManager() );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudBlobSupport<>(
                        AzureTarget.class,
                        BlobAzureTarget.class,
                        SuspectBlobAzureTarget.class,
                        AzureTargetReadPreference.class,
                        CollectionFactory.toSet( blob.getId() ),
                        null );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudBlobSupport<>(
                        AzureTarget.class,
                        BlobAzureTarget.class,
                        SuspectBlobAzureTarget.class,
                        AzureTargetReadPreference.class,
                        new HashSet< UUID >(),
                        dbSupport.getServiceManager() );
            }
        } );
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudBlobSupport<>(
                        AzureTarget.class,
                        BlobAzureTarget.class,
                        SuspectBlobAzureTarget.class,
                        AzureTargetReadPreference.class,
                        CollectionFactory.toSet( UUID.randomUUID() ),
                        dbSupport.getServiceManager() );
            }
        } );
    }
    
    
    @Test
    public void testGetBlobsNullReadPreferenceNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );

        final PublicCloudBlobSupport< ?, ?, ? > support = new PublicCloudBlobSupport<>(
                AzureTarget.class,
                BlobAzureTarget.class,
                SuspectBlobAzureTarget.class,
                AzureTargetReadPreference.class,
                CollectionFactory.toSet( blob.getId() ),
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    support.getBlobs( null );
                }
            } );
    }
    
    
    @Test
    public void testGetBlobsReturnsBlobsAsIsAppropriate()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "b1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b2" );
        final S3Object o11 = mockDaoDriver.createObject( bucket1.getId(), "o1" );
        final S3Object o12 = mockDaoDriver.createObject( bucket1.getId(), "o2" );
        final S3Object o21 = mockDaoDriver.createObject( bucket2.getId(), "o1" );
        final S3Object o22 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final S3Object o23 = mockDaoDriver.createObject( bucket2.getId(), "o3" );
        final Blob blob11 = mockDaoDriver.getBlobFor( o11.getId() );
        mockDaoDriver.getBlobFor( o12.getId() );
        final Blob blob21 = mockDaoDriver.getBlobFor( o21.getId() );
        final Blob blob22 = mockDaoDriver.getBlobFor( o22.getId() );
        final Blob blob23 = mockDaoDriver.getBlobFor( o23.getId() );
        
        final S3Target s3Target = mockDaoDriver.createS3Target( "t1" );
        mockDaoDriver.createAzureTarget( "t0" );
        final AzureTarget target1 = mockDaoDriver.createAzureTarget( "t1" );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( "t2" );
        final AzureTarget target3 = mockDaoDriver.createAzureTarget( "t3" );
        
        mockDaoDriver.updateBean(
                target1.setDefaultReadPreference( TargetReadPreferenceType.values()[ 3 ] ), 
                ReplicationTarget.DEFAULT_READ_PREFERENCE );
        mockDaoDriver.updateBean(
                target2.setDefaultReadPreference( TargetReadPreferenceType.values()[ 4 ] ), 
                ReplicationTarget.DEFAULT_READ_PREFERENCE );
        mockDaoDriver.updateBean(
                target3.setDefaultReadPreference( TargetReadPreferenceType.values()[ 4 ] ), 
                ReplicationTarget.DEFAULT_READ_PREFERENCE );
        mockDaoDriver.updateBean(
                target3.setState( TargetState.OFFLINE ), 
                ReplicationTarget.STATE );
        
        mockDaoDriver.createAzureTargetReadPreference(
                target1.getId(), bucket1.getId(), TargetReadPreferenceType.values()[ 1 ] );
        mockDaoDriver.createAzureTargetReadPreference(
                target2.getId(), bucket2.getId(), TargetReadPreferenceType.values()[ 2 ] );
        
        mockDaoDriver.putBlobOnS3Target( s3Target.getId(), blob11.getId() );
        mockDaoDriver.putBlobOnS3Target( s3Target.getId(), blob21.getId() );
        mockDaoDriver.putBlobOnAzureTarget( target1.getId(), blob11.getId() );
        mockDaoDriver.putBlobOnAzureTarget( target1.getId(), blob21.getId() );
        mockDaoDriver.putBlobOnAzureTarget( target2.getId(), blob21.getId() );
        mockDaoDriver.putBlobOnAzureTarget( target2.getId(), blob22.getId() );
        mockDaoDriver.putBlobOnAzureTarget( target2.getId(), blob23.getId() );
        mockDaoDriver.putBlobOnAzureTarget( target3.getId(), blob23.getId() );

        Map< UUID, Set< UUID > > result;
        final PublicCloudBlobSupport< ?, ?, ? > support = new PublicCloudBlobSupport<>(
                AzureTarget.class,
                BlobAzureTarget.class,
                SuspectBlobAzureTarget.class,
                AzureTargetReadPreference.class,
                CollectionFactory.toSet( blob21.getId(), blob23.getId() ),
                dbSupport.getServiceManager() );
        
        result = support.getBlobs( TargetReadPreferenceType.values()[ 0 ] );
        assertEquals(0,  result.size(), "Should notta been any candidate blobs for read preference.");

        result = support.getBlobs( TargetReadPreferenceType.values()[ 1 ] );
        assertEquals(0,  result.size(), "Should notta been any candidate blobs for read preference.");

        result = support.getBlobs( TargetReadPreferenceType.values()[ 2 ] );
        assertEquals(1,  result.size(), "Shoulda been candidate blobs for read preference.");
        final Object expected1 = target2.getId();
        assertEquals(expected1, result.keySet().iterator().next(), "Shoulda been candidate blobs for read preference.");
        assertEquals(2,  result.entrySet().iterator().next().getValue().size(), "Shoulda been candidate blobs for read preference.");

        result = support.getBlobs( TargetReadPreferenceType.values()[ 3 ] );
        assertEquals(1,  result.size(), "Shoulda been candidate blobs for read preference.");
        final Object expected = target1.getId();
        assertEquals(expected, result.keySet().iterator().next(), "Shoulda been candidate blobs for read preference.");
        assertEquals(1,  result.entrySet().iterator().next().getValue().size(), "Shoulda been candidate blobs for read preference.");

        result = support.getBlobs( TargetReadPreferenceType.values()[ 4 ] );
        assertEquals(0,  result.size(), "Should notta been any candidate blobs for read preference.");
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
