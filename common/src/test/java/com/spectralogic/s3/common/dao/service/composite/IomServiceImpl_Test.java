/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */

package com.spectralogic.s3.common.dao.service.composite;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataMigration;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacementRuleState;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.Obsoletion;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMemberState;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.ObsoleteBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.BlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.common.dao.orm.BlobRM;
import com.spectralogic.s3.common.dao.orm.TapeRM;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.iterate.CloseableIterable;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.marshal.DateMarshaler;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public class IomServiceImpl_Test 
{
    @Test
    public void testCleanupOldMigrations()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID getJobId = mockDaoDriver.createJob( null, null, JobRequestType.GET ).getId();
        final UUID putJobId = mockDaoDriver.createJob( null, null, JobRequestType.PUT ).getId();
        final DataMigration dm = BeanFactory.newBean( DataMigration.class )
                .setGetJobId( getJobId )
                .setPutJobId( putJobId );
        dbSupport.getServiceManager().getCreator( DataMigration.class ).create( dm );
        IomService iomService = new IomServiceImpl( dbSupport.getServiceManager() );
        
        assertNotNull( dbSupport.getServiceManager().getRetriever( DataMigration.class ).retrieve( dm.getId() ) );
        iomService.cleanupOldMigrations( new Consumer< DataMigration >() {
            
            @Override
            public void accept( DataMigration dm )
            {
                final Object expected1 = dm.getGetJobId();
                assertEquals(expected1, null, String.valueOf(getJobId) );
                final Object expected = dm.getPutJobId();
                assertEquals(expected, null, String.valueOf(putJobId));
            }
        } );
        assertNotNull( dbSupport.getServiceManager().getRetriever( DataMigration.class ).retrieve( dm.getId() ) );
        
        mockDaoDriver.updateBean( dm.setPutJobId( null ) , DataMigration.PUT_JOB_ID );
        mockDaoDriver.updateBean( dm.setGetJobId( null ) , DataMigration.GET_JOB_ID );
        iomService.cleanupOldMigrations( new Consumer< DataMigration >() {
            
            @Override
            public void accept( DataMigration dm )
            {
                assertNull( dm.getGetJobId() );
                assertNull( dm.getPutJobId() );
            }
        } );
        assertNull( dbSupport.getServiceManager().getRetriever( DataMigration.class ).retrieve( dm.getId() ) );
    }
    
    
    @Test
    public void testDeleteBlobRecordsThatHaveFinishedMigratingOrHealing()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createABMConfigSingleCopyOnTape();
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final Bucket backupBucket = mockDaoDriver.createBucket( null, dp.getId(), "Spectra-backup" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "obj1" );
        final UUID instanceId = mockDaoDriver.attainOneAndOnly( DataPathBackend.class ).getInstanceId();
        int time = 1;
        
        
        final Blob blob = mockDaoDriver.getBlobFor( o1.getId() );
        final Tape tape = mockDaoDriver
                .createTape( mockDaoDriver.attainOneAndOnly( TapePartition.class ).getId(), TapeState.NORMAL );
        
        final StorageDomain storageDomain = mockDaoDriver.attainOneAndOnly( StorageDomain.class );
        final IomService iomService = new IomServiceImpl( dbSupport.getServiceManager() );
        
        final BlobTape blobTape1 =
                mockDaoDriver.putBlobOnTapeForStorageDomain( tape.getId(), blob.getId(), storageDomain.getId() );

        iomService.deleteBlobRecordsThatHaveFinishedMigratingOrHealing();
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Should not have deleted any blobs.");

        final Obsoletion obsoletion = BeanFactory.newBean( Obsoletion.class ).setDate( new Date( time++ * 1000 ) );
        dbSupport.getServiceManager().getCreator( Obsoletion.class ).create( obsoletion );
        final ObsoleteBlobTape obsoleteBlobTape1 = BeanFactory.newBean( ObsoleteBlobTape.class );
        obsoleteBlobTape1.setObsoletionId( obsoletion.getId() ).setBlobId( blob.getId() ).setTapeId( tape.getId() )
                .setId( blobTape1.getId() );
        dbSupport.getServiceManager().getCreator( ObsoleteBlobTape.class ).create( obsoleteBlobTape1 );
        
        final BlobTape blobTape2 =
                mockDaoDriver.putBlobOnTapeForStorageDomain( tape.getId(), blob.getId(), storageDomain.getId() );

        iomService.deleteBlobRecordsThatHaveFinishedMigratingOrHealing();
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Should not have deleted obsolete blobs since no db backup.");
        final S3Object dbBackup = mockDaoDriver.createObject( backupBucket.getId(), "backup" );
        final Map< String, String > backupProperties = new HashMap<>();
        backupProperties.put( KeyValueObservable.BACKUP_START_DATE,
                DateMarshaler.marshal( new Date( time++ * 1000 ) ) );
        backupProperties.put( KeyValueObservable.BACKUP_INSTANCE_ID, instanceId.toString() );
        mockDaoDriver.createObjectProperties( dbBackup.getId(), backupProperties );
               
        iomService.deleteBlobRecordsThatHaveFinishedMigratingOrHealing();
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Should have deleted obsolete blobs.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(Obsoletion.class).getCount(), "Should have deleted obsoletion.");

        final Obsoletion obsoletion2 = BeanFactory.newBean( Obsoletion.class ).setDate( new Date( time++ * 1000 ) );
        dbSupport.getServiceManager().getCreator( Obsoletion.class ).create( obsoletion2 );
        final ObsoleteBlobTape obsoleteBlobTape2 = BeanFactory.newBean( ObsoleteBlobTape.class );
        obsoleteBlobTape2.setObsoletionId( obsoletion2.getId() ).setBlobId( blob.getId() ).setTapeId( tape.getId() )
                .setId( blobTape2.getId() );
        dbSupport.getServiceManager().getCreator( ObsoleteBlobTape.class ).create( obsoleteBlobTape2 );
        
        iomService.deleteBlobRecordsThatHaveFinishedMigratingOrHealing();
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Should not have deleted obsolete blobs since db backup is before obsoletion.");

        final S3Object dbBackup2 = mockDaoDriver.createObject( backupBucket.getId(), "backup2" );
        final Map< String, String > backupProperties2 = new HashMap<>();
        backupProperties2.put( KeyValueObservable.BACKUP_START_DATE,
                DateMarshaler.marshal( new Date( time++ * 1000 ) ) );
        backupProperties2.put( KeyValueObservable.BACKUP_INSTANCE_ID, instanceId.toString() );
        mockDaoDriver.createObjectProperties( dbBackup2.getId(), backupProperties2 );
        
        iomService.deleteBlobRecordsThatHaveFinishedMigratingOrHealing();
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(), "Should have unobsoleted blob and not deleted since we have no other copies.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(ObsoleteBlobTape.class).getCount(), "Should have deleted obsoleteBlobTape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(Obsoletion.class).getCount(), "Should have deleted obsoletion.");
    }
    
    
    @Test
    public void testGetBlobsRequiringLocalIOMWork()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createABMConfigSingleCopyOnTape();
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "obj1" );
        final Blob blob = mockDaoDriver.getBlobFor( o1.getId() );
        final Tape tape = mockDaoDriver
                .createTape( mockDaoDriver.attainOneAndOnly( TapePartition.class ).getId(), TapeState.NORMAL );

        final StorageDomain storageDomain = mockDaoDriver.attainOneAndOnly( StorageDomain.class );
        final IomService iomService = new IomServiceImpl( dbSupport.getServiceManager() );
        final BlobTape blobTape =
                mockDaoDriver.putBlobOnTapeForStorageDomain( tape.getId(), blob.getId(), storageDomain.getId() );
        final DataPersistenceRule originalDPR = mockDaoDriver.attainOneAndOnly( DataPersistenceRule.class );

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringLocalIOMWork( bucket.getId(), originalDPR ) );

        final StorageDomainMember sdm =
                new TapeRM( tape.getId(), dbSupport.getServiceManager() ).getStorageDomainMember().unwrap();
        mockDaoDriver.updateBean(
                sdm.setState( StorageDomainMemberState.EXCLUSION_IN_PROGRESS ),
                StorageDomainMember.STATE );

        assertBlobCount(
                "IOM work should be required for member pending exclusion",
                1,
                iomService.getBlobsRequiringLocalIOMWork( bucket.getId(), originalDPR ) );
        
        mockDaoDriver.updateBean(
                sdm.setState( StorageDomainMemberState.NORMAL ),
                StorageDomainMember.STATE );

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringLocalIOMWork( bucket.getId(), originalDPR ) );

        mockDaoDriver.updateBean(
                tape.setState( TapeState.AUTO_COMPACTION_IN_PROGRESS ),
                StorageDomainMember.STATE );
                
        assertBlobCount(
                "IOM work should be required for member pending exclusion.",
                1,
                iomService.getBlobsRequiringLocalIOMWork( bucket.getId(), originalDPR ) );
                
        mockDaoDriver.updateBean(
                tape.setState( TapeState.NORMAL ),
                StorageDomainMember.STATE );
                
        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringLocalIOMWork( bucket.getId(), originalDPR ) );
                
        final SuspectBlobTape suspect = mockDaoDriver.makeSuspect( blobTape );
        
        assertBlobCount(
                "IOM work should be required for suspect blob tape.",
                1,
                iomService.getBlobsRequiringLocalIOMWork( bucket.getId(), originalDPR ) );
                
        mockDaoDriver.delete( SuspectBlobTape.class, suspect);
        
        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringLocalIOMWork( bucket.getId(), originalDPR ) );

        final StorageDomain sd2 = mockDaoDriver.createStorageDomain("sd2");
        final DataPersistenceRule dpr = mockDaoDriver.createDataPersistenceRule( dp.getId(), DataPersistenceRuleType.PERMANENT, sd2.getId() );
        final DegradedBlob degraded = mockDaoDriver.createDegradedBlob( blob.getId(), dpr.getId() );

        assertBlobCount(
                "Should have had IOM work to do for new degraded blob.",
                1,
                iomService.getBlobsRequiringLocalIOMWork( bucket.getId(), dpr ) );

        mockDaoDriver.delete( DegradedBlob.class, degraded );

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringLocalIOMWork( bucket.getId(), originalDPR ) );

        mockDaoDriver.updateBean(
                dpr.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS ),
                DataPersistenceRule.STATE );

        assertBlobCount(
                "Should not have had IOM work to do for storage domain where data is already persisted.",
                0,
                iomService.getBlobsRequiringLocalIOMWork( bucket.getId(), originalDPR ) );
        
        assertBlobCount(
                "Should have had IOM work to do for new permanent persistence rule.",
                1,
                iomService.getBlobsRequiringLocalIOMWork( bucket.getId(), dpr ) );

        final Job putJob = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.createJobEntry( putJob.getId(), blob);

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringLocalIOMWork( bucket.getId(), originalDPR ) );
    }


    @Test
    public void testGetBlobsRequiringIOMWorkOnAzureTarget()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createABMConfigTemporaryOnly();
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "obj1" );
        final Blob blobForAzure = mockDaoDriver.getBlobFor( o1.getId() );
        final AzureTarget azureTarget = mockDaoDriver.createAzureTarget( "azure" );

        final AzureDataReplicationRule adpr = mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(),
                DataReplicationRuleType.PERMANENT,
                azureTarget.getId() );
        mockDaoDriver.updateBean(
                adpr.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS ),
                DataReplicationRule.STATE );

        final IomService iomService = new IomServiceImpl( dbSupport.getServiceManager() );
        assertBlobCount(
                "IOM work should be needed for blob not replicated",
                1,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobAzureTarget.class,
                        SuspectBlobAzureTarget.class,
                        bucket.getId(),
                        azureTarget.getId(),
                        bucket.getDataPolicyId() ) );

        final BlobAzureTarget azureBlob =
                mockDaoDriver.putBlobOnAzureTarget( azureTarget.getId(), blobForAzure.getId() );

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobAzureTarget.class,
                        SuspectBlobAzureTarget.class,
                        bucket.getId(),
                        azureTarget.getId(),
                        bucket.getDataPolicyId() ) );

        final SuspectBlobAzureTarget suspect = mockDaoDriver.makeSuspect( azureBlob );

        assertBlobCount(
                "IOM work should be required for suspect blob target.",
                1,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobAzureTarget.class,
                        SuspectBlobAzureTarget.class,
                        bucket.getId(),
                        azureTarget.getId(),
                        bucket.getDataPolicyId() ) );

        mockDaoDriver.delete( SuspectBlobAzureTarget.class, suspect);

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobAzureTarget.class,
                        SuspectBlobAzureTarget.class,
                        bucket.getId(),
                        azureTarget.getId(),
                        bucket.getDataPolicyId() ) );

        final Job putJob = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.createJobEntry( putJob.getId(), blobForAzure );

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobAzureTarget.class,
                        SuspectBlobAzureTarget.class,
                        bucket.getId(),
                        azureTarget.getId(),
                        bucket.getDataPolicyId() ) );

        mockDaoDriver.delete( Job.class, putJob );
        final DegradedBlob degraded = mockDaoDriver.createDegradedBlob( blobForAzure.getId(), adpr.getId() );
        mockDaoDriver.delete( BlobAzureTarget.class, azureBlob );

        assertBlobCount(
                "Should have had IOM work to do for new degraded blob.",
                1,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobAzureTarget.class,
                        SuspectBlobAzureTarget.class,
                        bucket.getId(),
                        azureTarget.getId(),
                        bucket.getDataPolicyId() ) );

        mockDaoDriver.delete( DegradedBlob.class, degraded );
        final BlobAzureTarget azureBlob2 =
                mockDaoDriver.putBlobOnAzureTarget( azureTarget.getId(), blobForAzure.getId() );

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobAzureTarget.class,
                        SuspectBlobAzureTarget.class,
                        bucket.getId(),
                        azureTarget.getId(),
                        bucket.getDataPolicyId() ) );
    }
    
    
    @Test
    public void testHandleDataPlacementRulesFinishedPendingInclusionWorksForTargets()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createABMConfigTemporaryOnly();
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "obj1" );
        final Blob blobForAzure = mockDaoDriver.getBlobFor( o1.getId() );
        final AzureTarget azureTarget = mockDaoDriver.createAzureTarget( "azure" );
        final Blob blobForDs3 = mockDaoDriver.getBlobFor( o1.getId() );
        final Ds3Target ds3Target = mockDaoDriver.createDs3Target( "DS3" );
        final Blob blobForS3 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Target s3Target = mockDaoDriver.createS3Target( "S3" );
        

        final AzureDataReplicationRule adpr = mockDaoDriver.createAzureDataReplicationRule(
                dp.getId(),
                DataReplicationRuleType.PERMANENT,
                azureTarget.getId() );
        mockDaoDriver.updateBean(
                adpr.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS ),
                DataPlacement.STATE );
        final Ds3DataReplicationRule ddpr = mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(),
                DataReplicationRuleType.PERMANENT,
                ds3Target.getId() );
        mockDaoDriver.updateBean(
                ddpr.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS ),
                DataPlacement.STATE );
        final S3DataReplicationRule sdpr = mockDaoDriver.createS3DataReplicationRule(
                dp.getId(),
                DataReplicationRuleType.PERMANENT,
                s3Target.getId() );
        mockDaoDriver.updateBean(
                sdpr.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS ),
                DataPlacement.STATE );


        final IomService iomService = new IomServiceImpl( dbSupport.getServiceManager() );
        
        iomService.handleDataPlacementRulesFinishedPendingInclusion();

        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain(adpr).getState(), "Should not have changed rule state.");

        mockDaoDriver.putBlobOnAzureTarget( azureTarget.getId(), blobForAzure.getId() );
        iomService.handleDataPlacementRulesFinishedPendingInclusion();

        assertEquals(DataPlacementRuleState.NORMAL, mockDaoDriver.attain(adpr).getState(), "Should have marked rule normal.");
        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain(ddpr).getState(), "Should not have changed rule state.");
        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain(sdpr).getState(), "Should not have changed rule state.");

        mockDaoDriver.putBlobOnDs3Target( ds3Target.getId(), blobForDs3.getId() );
        iomService.handleDataPlacementRulesFinishedPendingInclusion();

        assertEquals(DataPlacementRuleState.NORMAL, mockDaoDriver.attain(ddpr).getState(), "Should have marked rule normal.");
        assertEquals(DataPlacementRuleState.INCLUSION_IN_PROGRESS, mockDaoDriver.attain(sdpr).getState(), "Should not have changed rule state.");

        mockDaoDriver.putBlobOnS3Target( s3Target.getId(), blobForS3.getId() );
        iomService.handleDataPlacementRulesFinishedPendingInclusion();

        assertEquals(DataPlacementRuleState.NORMAL, mockDaoDriver.attain(sdpr).getState(), "Should have marked rule normal.");
    }


    @Test
    public void testGetBlobsRequiringIOMWorkOnDs3Target()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createABMConfigTemporaryOnly();
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "obj1" );
        final Blob blobForDs3 = mockDaoDriver.getBlobFor( o1.getId() );
        final Ds3Target ds3Target = mockDaoDriver.createDs3Target( "DS3" );

        final Ds3DataReplicationRule ddpr = mockDaoDriver.createDs3DataReplicationRule(
                dp.getId(),
                DataReplicationRuleType.PERMANENT,
                ds3Target.getId() );
        mockDaoDriver.updateBean(
                ddpr.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS ),
                DataReplicationRule.STATE );

        final IomService iomService = new IomServiceImpl( dbSupport.getServiceManager() );
        assertBlobCount(
                "IOM work should be needed for blob not replicated",
                1,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobDs3Target.class,
                        SuspectBlobDs3Target.class,
                        bucket.getId(),
                        ds3Target.getId(),
                        bucket.getDataPolicyId() ) );

        final BlobDs3Target ds3Blob =
                mockDaoDriver.putBlobOnDs3Target( ds3Target.getId(), blobForDs3.getId() );

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobDs3Target.class,
                        SuspectBlobDs3Target.class,
                        bucket.getId(),
                        ds3Target.getId(),
                        bucket.getDataPolicyId() ) );

        final SuspectBlobDs3Target suspect = mockDaoDriver.makeSuspect( ds3Blob );

        assertBlobCount(
                "IOM work should be required for suspect blob target.",
                1,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobDs3Target.class,
                        SuspectBlobDs3Target.class,
                        bucket.getId(),
                        ds3Target.getId(),
                        bucket.getDataPolicyId() ) );

        mockDaoDriver.delete( SuspectBlobDs3Target.class, suspect);

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobDs3Target.class,
                        SuspectBlobDs3Target.class,
                        bucket.getId(),
                        ds3Target.getId(),
                        bucket.getDataPolicyId() ) );

        final DegradedBlob degraded = mockDaoDriver.createDegradedBlob( blobForDs3.getId(), ddpr.getId() );
        final BlobDs3Target blobRecord = new BlobRM(blobForDs3, dbSupport.getServiceManager()).getBlobDs3Targets().getFirst();
        mockDaoDriver.delete( BlobDs3Target.class, blobRecord);

        assertBlobCount(
                "Should have had IOM work to do for new degraded blob.",
                1,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobDs3Target.class,
                        SuspectBlobDs3Target.class,
                        bucket.getId(),
                        ds3Target.getId(),
                        bucket.getDataPolicyId() ) );

        mockDaoDriver.delete( DegradedBlob.class, degraded );
        dbSupport.getDataManager().createBean(blobRecord);

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobDs3Target.class,
                        SuspectBlobDs3Target.class,
                        bucket.getId(),
                        ds3Target.getId(),
                        bucket.getDataPolicyId() ) );

        final Job putJob = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.createJobEntry( putJob.getId(), blobForDs3 );

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobDs3Target.class,
                        SuspectBlobDs3Target.class,
                        bucket.getId(),
                        ds3Target.getId(),
                        bucket.getDataPolicyId() ) );
    }


    @Test
    public void testGetBlobsRequiringIOMWorkOnS3Target()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createABMConfigTemporaryOnly();
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), "bucket" );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "obj1" );
        final Blob blobForS3 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Target s3Target = mockDaoDriver.createS3Target( "S3" );

        final S3DataReplicationRule sdpr = mockDaoDriver.createS3DataReplicationRule(
                dp.getId(),
                DataReplicationRuleType.PERMANENT,
                s3Target.getId() );
        mockDaoDriver.updateBean(
                sdpr.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS ),
                DataReplicationRule.STATE );

        final IomService iomService = new IomServiceImpl( dbSupport.getServiceManager() );
        assertBlobCount(
                "IOM work should be needed for blob not replicated",
                1,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobS3Target.class,
                        SuspectBlobS3Target.class,
                        bucket.getId(),
                        s3Target.getId(),
                        bucket.getDataPolicyId() ) );

        final BlobS3Target s3Blob =
                mockDaoDriver.putBlobOnS3Target( s3Target.getId(), blobForS3.getId() );

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobS3Target.class,
                        SuspectBlobS3Target.class,
                        bucket.getId(),
                        s3Target.getId(),
                        bucket.getDataPolicyId() ) );

        final SuspectBlobS3Target suspect = mockDaoDriver.makeSuspect( s3Blob );

        assertBlobCount(
                "IOM work should be required for suspect blob target.",
                1,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobS3Target.class,
                        SuspectBlobS3Target.class,
                        bucket.getId(),
                        s3Target.getId(),
                        bucket.getDataPolicyId() ) );

        mockDaoDriver.delete( SuspectBlobS3Target.class, suspect);

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobS3Target.class,
                        SuspectBlobS3Target.class,
                        bucket.getId(),
                        s3Target.getId(),
                        bucket.getDataPolicyId() ) );


        final DegradedBlob degraded = mockDaoDriver.createDegradedBlob( blobForS3.getId(), sdpr.getId() );
        final BlobS3Target blobRecord = new BlobRM(blobForS3, dbSupport.getServiceManager()).getBlobS3Targets().getFirst();
        mockDaoDriver.delete( BlobS3Target.class, blobRecord);
        assertBlobCount(
                "Should have had IOM work to do for new degraded blob.",
                1,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobS3Target.class,
                        SuspectBlobS3Target.class,
                        bucket.getId(),
                        s3Target.getId(),
                        bucket.getDataPolicyId() ) );

        mockDaoDriver.delete( DegradedBlob.class, degraded );
        dbSupport.getDataManager().createBean(blobRecord);

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobS3Target.class,
                        SuspectBlobS3Target.class,
                        bucket.getId(),
                        s3Target.getId(),
                        bucket.getDataPolicyId() ) );

        final Job putJob = mockDaoDriver.createJob( bucket.getId(), null, JobRequestType.PUT );
        mockDaoDriver.createJobEntry( putJob.getId(), blobForS3 );

        assertBlobCount(
                "Should not have had IOM work to do.",
                0,
                iomService.getBlobsRequiringIOMWorkOnTarget(
                        BlobS3Target.class,
                        SuspectBlobS3Target.class,
                        bucket.getId(),
                        s3Target.getId(),
                        bucket.getDataPolicyId() ) );
    }

    
    private void assertBlobCount(
            final String message,
            final int blobCount,
            final CloseableIterable< Set< Blob > > batchIterable )
    {
        int batchCount = 0;
        for ( Set< Blob > batch : batchIterable )
        {
            if ( batchCount++ > 0)
            {
                fail( "assertBlobCount() assumes it will only be called with single-batch iterables.");
            }
            assertEquals(blobCount,  batch.size(), message);
        }
        if ( 0 == batchCount && 0 != blobCount )
        {
            fail( "Expected " + blobCount + " blobs to need IOM work, but there were none." );
        }
    }

    private void assertBlobCount(
            final String message,
            final int blobCount,
            final EnhancedIterable< Blob > batchIterable )
    {
        int count = 0;
        for ( Blob blob : batchIterable) {
            count++;
        }
        assertEquals(blobCount,  count, message);
    }

    private static DatabaseSupport dbSupport;

    @BeforeAll
    public static void setDBFactory() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    }

    @AfterEach
    public  void setUp() {
        dbSupport.reset();
    }
}
