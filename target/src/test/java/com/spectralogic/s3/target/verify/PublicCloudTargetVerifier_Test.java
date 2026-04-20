/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.verify;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.AzureTargetFailure;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.target.AzureTargetBucketNameService;
import com.spectralogic.s3.common.dao.service.target.AzureTargetFailureService;
import com.spectralogic.s3.common.dao.service.temp.BlobAzureTargetToVerifyService;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.target.BucketOnPublicCloud;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.common.testfrmwrk.target.MockAzureConnection;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class PublicCloudTargetVerifier_Test
{
    @Test
    public void testConstructorNullParamsNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        final MockAzureConnection connection = new MockAzureConnection(
                dbSupport.getServiceManager(), new ArrayList< BucketOnPublicCloud >() );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudTargetVerifier<>(
                        null, 
                        AzureDataReplicationRule.class,
                        target.getId(), 
                        connection.toConnectionFactory(), 
                        dbSupport.getServiceManager(), 
                        BlobAzureTargetToVerifyService.class, 
                        AzureTargetBucketNameService.class,
                        AzureTargetFailureService.class );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudTargetVerifier<>(
                        AzureTarget.class, 
                        null,
                        target.getId(), 
                        connection.toConnectionFactory(), 
                        dbSupport.getServiceManager(), 
                        BlobAzureTargetToVerifyService.class, 
                        AzureTargetBucketNameService.class,
                        AzureTargetFailureService.class );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudTargetVerifier<>(
                        AzureTarget.class, 
                        AzureDataReplicationRule.class,
                        null, 
                        connection.toConnectionFactory(), 
                        dbSupport.getServiceManager(), 
                        BlobAzureTargetToVerifyService.class, 
                        AzureTargetBucketNameService.class,
                        AzureTargetFailureService.class );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudTargetVerifier<>(
                        AzureTarget.class, 
                        AzureDataReplicationRule.class,
                        target.getId(), 
                        null, 
                        dbSupport.getServiceManager(), 
                        BlobAzureTargetToVerifyService.class, 
                        AzureTargetBucketNameService.class,
                        AzureTargetFailureService.class );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudTargetVerifier<>(
                        AzureTarget.class, 
                        AzureDataReplicationRule.class,
                        target.getId(), 
                        connection.toConnectionFactory(), 
                        null, 
                        BlobAzureTargetToVerifyService.class, 
                        AzureTargetBucketNameService.class,
                        AzureTargetFailureService.class );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudTargetVerifier<>(
                        AzureTarget.class, 
                        AzureDataReplicationRule.class,
                        target.getId(), 
                        connection.toConnectionFactory(), 
                        dbSupport.getServiceManager(), 
                        null, 
                        AzureTargetBucketNameService.class,
                        AzureTargetFailureService.class );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudTargetVerifier<>(
                        AzureTarget.class, 
                        AzureDataReplicationRule.class,
                        target.getId(), 
                        connection.toConnectionFactory(), 
                        dbSupport.getServiceManager(), 
                        BlobAzureTargetToVerifyService.class, 
                        null,
                        AzureTargetFailureService.class );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PublicCloudTargetVerifier<>(
                        AzureTarget.class, 
                        AzureDataReplicationRule.class,
                        target.getId(), 
                        connection.toConnectionFactory(), 
                        dbSupport.getServiceManager(), 
                        BlobAzureTargetToVerifyService.class, 
                        AzureTargetBucketNameService.class,
                        null );
            }
        } );
        new PublicCloudTargetVerifier<>(
                AzureTarget.class, 
                AzureDataReplicationRule.class,
                target.getId(), 
                connection.toConnectionFactory(), 
                dbSupport.getServiceManager(), 
                BlobAzureTargetToVerifyService.class, 
                AzureTargetBucketNameService.class,
                AzureTargetFailureService.class );
    }

    @Test
    public void testRunWhenVerifyAlreadyRunningNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        
        final List< BucketOnPublicCloud > discoverableSegments =
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 3 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createAzureDataReplicationRule( dataPolicy.getId(), null, target.getId() );
        final MockAzureConnection connection = 
                new MockAzureConnection( dbSupport.getServiceManager(), discoverableSegments );
        connection.setSimulatedDelay( 1000 );
        
        final PublicCloudTargetVerifier< ?, ?, ? > verifier = new PublicCloudTargetVerifier<>(
                AzureTarget.class, 
                AzureDataReplicationRule.class,
                target.getId(), 
                connection.toConnectionFactory(), 
                dbSupport.getServiceManager(), 
                BlobAzureTargetToVerifyService.class, 
                AzureTargetBucketNameService.class,
                AzureTargetFailureService.class );
        final WorkPool wp = WorkPoolFactory.createWorkPool( 1, getClass().getSimpleName() );
        wp.submit( verifier );
        TestUtil.sleep( 10 );
        TestUtil.assertThrows( null, GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                verifier.run();
            }
        } );
        wp.shutdownNow();
        
        connection.assertNoDeletesRequested( false );
    }

    @Test
    public void testRunWhenBucketTargetsNonOwnedBucketInCloudThrows()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        
        final List< BucketOnPublicCloud > discoverableSegments = 
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 3 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createAzureDataReplicationRule( dataPolicy.getId(), null, target.getId() );
        final MockAzureConnection connection = 
                new MockAzureConnection( dbSupport.getServiceManager(), discoverableSegments );
        connection.setOwnerId( UUID.randomUUID() );
        
        final PublicCloudTargetVerifier< ?, ?, ? > verifier = new PublicCloudTargetVerifier<>(
                AzureTarget.class, 
                AzureDataReplicationRule.class,
                target.getId(), 
                connection.toConnectionFactory(), 
                dbSupport.getServiceManager(), 
                BlobAzureTargetToVerifyService.class, 
                AzureTargetBucketNameService.class,
                AzureTargetFailureService.class );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test() throws Throwable
            {
                verifier.run();
            }
        } );
        
        connection.assertNoDeletesRequested( true );
        assertEquals(TargetFailureType.VERIFY_FAILED, mockDaoDriver.attainOneAndOnly( AzureTargetFailure.class ).getType(), "Shoulda generated a target failure for the verify failure.");
    }

    @Test
    public void testRunWhenBucketNotTargetingAzureTargetBeingVerifiedDoesNothing()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );
        
        final List< BucketOnPublicCloud > discoverableSegments = 
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 3 );
        final MockAzureConnection connection = 
                new MockAzureConnection( dbSupport.getServiceManager(), discoverableSegments );
        
        final PublicCloudTargetVerifier< ?, ?, ? > verifier = new PublicCloudTargetVerifier<>(
                AzureTarget.class, 
                AzureDataReplicationRule.class,
                target.getId(), 
                connection.toConnectionFactory(), 
                dbSupport.getServiceManager(), 
                BlobAzureTargetToVerifyService.class, 
                AzureTargetBucketNameService.class,
                AzureTargetFailureService.class );
        verifier.run();
        
        connection.assertNoDeletesRequested( false );
    }

    @Test
    public void testRunWhenUnknownBlobsOnTargetAndReplicateDeletesEnabledDoesIssueDeleteRequests()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );

        final List< BucketOnPublicCloud > discoverableSegments = 
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 3 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        mockDaoDriver.createAzureDataReplicationRule( dataPolicy.getId(), null, target.getId() );
        final MockAzureConnection connection = 
                new MockAzureConnection( dbSupport.getServiceManager(), discoverableSegments );

        final S3ObjectOnMedia o1 = discoverableSegments.get( 0 ).getObjects()[ 0 ];
        final BlobOnMedia b1 = discoverableSegments.get( 0 ).getObjects()[ 0 ].getBlobs()[ 0 ];
        dbSupport.getDataManager().deleteBeans(
                BlobAzureTarget.class, 
                Require.beanPropertyEquals( BlobObservable.BLOB_ID, b1.getId() ) );
        
        final PublicCloudTargetVerifier< ?, ?, ? > verifier = new PublicCloudTargetVerifier<>(
                AzureTarget.class, 
                AzureDataReplicationRule.class,
                target.getId(), 
                connection.toConnectionFactory(), 
                dbSupport.getServiceManager(), 
                BlobAzureTargetToVerifyService.class, 
                AzureTargetBucketNameService.class,
                AzureTargetFailureService.class );
        verifier.run();
        
        connection.assertDeletesRequestedCorrectly( CollectionFactory.toSet( o1.getId() ) );
        assertEquals(0, dbSupport.getServiceManager().getRetriever(SuspectBlobAzureTarget.class).getCount(), "Should notta recorded suspect blob loss.");
    }

    @Test
    public void testRunWhenUnknownBlobsOnTargetAndReplicateDeletesDisabledDoesNotIssueDeleteRequests()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );

        final List< BucketOnPublicCloud > discoverableSegments =
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 3 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final AzureDataReplicationRule rule = mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        mockDaoDriver.updateBean( 
                rule.setReplicateDeletes( false ), 
                DataReplicationRule.REPLICATE_DELETES );
        final MockAzureConnection connection = 
                new MockAzureConnection( dbSupport.getServiceManager(), discoverableSegments );

        final BlobOnMedia b1 = discoverableSegments.get( 0 ).getObjects()[ 0 ].getBlobs()[ 0 ];
        dbSupport.getDataManager().deleteBeans(
                BlobAzureTarget.class, 
                Require.beanPropertyEquals( BlobObservable.BLOB_ID, b1.getId() ) );
        
        final PublicCloudTargetVerifier< ?, ?, ? > verifier = new PublicCloudTargetVerifier<>(
                AzureTarget.class, 
                AzureDataReplicationRule.class,
                target.getId(), 
                connection.toConnectionFactory(), 
                dbSupport.getServiceManager(), 
                BlobAzureTargetToVerifyService.class, 
                AzureTargetBucketNameService.class,
                AzureTargetFailureService.class );
        verifier.run();
        
        connection.assertDeletesRequestedCorrectly( new HashSet< UUID >() );
        assertEquals(0, dbSupport.getServiceManager().getRetriever(SuspectBlobAzureTarget.class).getCount(), "Should notta recorded suspect blob loss.");
    }

    @Test
    public void testRunWhenDataCorruptedInCloudResultsInSuspectBlobs()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );

        final List< BucketOnPublicCloud > discoverableSegments = 
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 3 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final AzureDataReplicationRule rule = mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        mockDaoDriver.updateBean( 
                rule.setReplicateDeletes( false ), 
                DataReplicationRule.REPLICATE_DELETES );
        final MockAzureConnection connection = 
                new MockAzureConnection( dbSupport.getServiceManager(), discoverableSegments );

        final BlobOnMedia b1 = discoverableSegments.get( 0 ).getObjects()[ 0 ].getBlobs()[ 0 ];
        b1.setId( UUID.randomUUID() );
        
        final PublicCloudTargetVerifier< ?, ?, ? > verifier = new PublicCloudTargetVerifier<>(
                AzureTarget.class, 
                AzureDataReplicationRule.class,
                target.getId(), 
                connection.toConnectionFactory(), 
                dbSupport.getServiceManager(), 
                BlobAzureTargetToVerifyService.class, 
                AzureTargetBucketNameService.class,
                AzureTargetFailureService.class );
        verifier.run();
        
        connection.assertDeletesRequestedCorrectly( new HashSet< UUID >() );
        assertEquals(1, dbSupport.getServiceManager().getRetriever(SuspectBlobAzureTarget.class).getCount(), "Shoulda recorded suspect blob loss.");
    }

    @Test
    public void testRunWhenBucketMissingInCloudResultsInSuspectBlobs()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );

        MockAzureConnection.createDiscoverableSegments( dbSupport, target, 3 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final AzureDataReplicationRule rule = mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        mockDaoDriver.updateBean( 
                rule.setReplicateDeletes( false ), 
                DataReplicationRule.REPLICATE_DELETES );
        final MockAzureConnection connection = new MockAzureConnection(
                dbSupport.getServiceManager(), 
                new ArrayList< BucketOnPublicCloud >() );
        connection.cloudBucketDoesNotExist();
        
        final PublicCloudTargetVerifier< ?, ?, ? > verifier = new PublicCloudTargetVerifier<>(
                AzureTarget.class, 
                AzureDataReplicationRule.class,
                target.getId(), 
                connection.toConnectionFactory(), 
                dbSupport.getServiceManager(), 
                BlobAzureTargetToVerifyService.class, 
                AzureTargetBucketNameService.class,
                AzureTargetFailureService.class );
        verifier.run();

        connection.assertDeletesRequestedCorrectly( new HashSet< UUID >() );
        assertTrue(0 < dbSupport.getServiceManager().getRetriever( SuspectBlobAzureTarget.class ).getCount(), "Shoulda recorded suspect blob loss.");
    }

    @Test
    public void testRunWhenDataMissingInCloudResultsInSuspectBlobs()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target = mockDaoDriver.createAzureTarget( "t1" );

        final List< BucketOnPublicCloud > discoverableSegments = 
                MockAzureConnection.createDiscoverableSegments( dbSupport, target, 3 );
        discoverableSegments.remove( 0 );
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final AzureDataReplicationRule rule = mockDaoDriver.createAzureDataReplicationRule(
                dataPolicy.getId(), null, target.getId() );
        mockDaoDriver.updateBean( 
                rule.setReplicateDeletes( false ), 
                DataReplicationRule.REPLICATE_DELETES );
        final MockAzureConnection connection = 
                new MockAzureConnection( dbSupport.getServiceManager(), discoverableSegments );
        
        final PublicCloudTargetVerifier< ?, ?, ? > verifier = new PublicCloudTargetVerifier<>(
                AzureTarget.class, 
                AzureDataReplicationRule.class,
                target.getId(), 
                connection.toConnectionFactory(), 
                dbSupport.getServiceManager(), 
                BlobAzureTargetToVerifyService.class, 
                AzureTargetBucketNameService.class,
                AzureTargetFailureService.class );
        verifier.run();
        
        connection.assertDeletesRequestedCorrectly( new HashSet< UUID >() );
        assertTrue(0 < dbSupport.getServiceManager().getRetriever( SuspectBlobAzureTarget.class ).getCount(), "Shoulda recorded suspect blob loss.");
    }
}
