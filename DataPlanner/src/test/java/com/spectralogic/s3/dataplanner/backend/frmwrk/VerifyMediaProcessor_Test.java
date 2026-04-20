/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.frmwrk;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolBlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeBlobStore;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.*;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.*;


public final class VerifyMediaProcessor_Test
{
    @Test
    public void testEligibleTapesForAutomaticVerificationAreVerified()
    {
        final Method methodVerify = ReflectUtil.getMethod( BlobStore.class, "verify" );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain storageDomain2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.updateBean( 
                storageDomain.setMaximumAutoVerificationFrequencyInDays( Integer.valueOf( 30 ) ), 
                StorageDomain.MAXIMUM_AUTO_VERIFICATION_FREQUENCY_IN_DAYS );
        final Tape tape0 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape3 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape4 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape5 = mockDaoDriver.createTape( TapeState.DATA_CHECKPOINT_FAILURE );
        final Tape tape6 = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape0.getPartitionId(), tape0.getType() );
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain2.getId(), tape0.getPartitionId(), tape0.getType() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.putBlobOnTape( tape0.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnTape( tape3.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnTape( tape4.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnTape( tape5.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnTape( tape6.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.createTapeDrive( null, "tdsn1" );
        mockDaoDriver.createTapeDrive( null, "tdsn2" );
        mockDaoDriver.createTapeDrive( null, "tdsn3" );
        mockDaoDriver.createTapeDrive( null, "tdsn4" );
        mockDaoDriver.updateBean( 
                tape0.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        mockDaoDriver.updateBean( 
                tape1.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        mockDaoDriver.updateBean( 
                tape2.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ).setLastVerified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED, PersistenceTarget.LAST_VERIFIED );
        mockDaoDriver.updateBean( 
                tape3.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        mockDaoDriver.updateBean( 
                tape4.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ).setLastVerified( new Date() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED, PersistenceTarget.LAST_VERIFIED );
        mockDaoDriver.updateBean( 
                tape5.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        mockDaoDriver.updateBean( 
                tape6.setStorageDomainMemberId( sdm2.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 1000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBlobStoreIh( 64, 0 ) );
        final TapeBlobStore blobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final VerifyMediaProcessor< Tape, BlobTape, VerifyTask > processor = new VerifyMediaProcessor<>( 
                Tape.class,
                Require.not( Require.beanPropertyEqualsOneOf( 
                        Tape.STATE, TapeState.getStatesThatDisallowTapeLoadIntoDrive() ) ),
                BlobTape.class,
                VerifyTask.class,
                dbSupport.getServiceManager(), 
                blobStore, 
                100,
                64 );
        
        TestUtil.sleep( 150 );
        List< MethodInvokeData > invocations = btih.getMethodInvokeData( methodVerify );
        assertEquals( 2,  invocations.size(), "Shoulda made invocations on blob store to verify tape.");

        TestUtil.sleep( 100 );
        invocations = btih.getMethodInvokeData( methodVerify );
        assertEquals( 4,  invocations.size(), "Shoulda made invocations on blob store to verify tape.");

        assertNotNull(processor, "Don't prematurely gc me.");
        processor.shutdown();
    }
    
    
    @Test
    public void testEligibleTapesForAutomaticVerificationAreNotVerifiedWhenTooManyVerifyTasksQueued() throws InterruptedException {
        final Method methodVerify = ReflectUtil.getMethod( BlobStore.class, "verify" );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        mockDaoDriver.updateBean( 
                storageDomain.setMaximumAutoVerificationFrequencyInDays(30),
                StorageDomain.MAXIMUM_AUTO_VERIFICATION_FREQUENCY_IN_DAYS );
        final Tape tape0 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape3 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape4 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape5 = mockDaoDriver.createTape( TapeState.DATA_CHECKPOINT_FAILURE );
        mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape0.getPartitionId(), tape0.getType() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.putBlobOnTape( tape0.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnTape( tape3.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnTape( tape4.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnTape( tape5.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.createTapeDrive( null, "tdsn1" );
        mockDaoDriver.createTapeDrive( null, "tdsn2" );
        mockDaoDriver.createTapeDrive( null, "tdsn3" );
        mockDaoDriver.createTapeDrive( null, "tdsn4" );
        mockDaoDriver.updateBean( 
                tape0.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        mockDaoDriver.updateBean( 
                tape1.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        mockDaoDriver.updateBean( 
                tape2.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ).setLastVerified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED, PersistenceTarget.LAST_VERIFIED );
        mockDaoDriver.updateBean( 
                tape3.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        mockDaoDriver.updateBean( 
                tape4.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ).setLastVerified( new Date() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED, PersistenceTarget.LAST_VERIFIED );
        mockDaoDriver.updateBean( 
                tape5.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBlobStoreIh( 4, 63 ) );
        final TapeBlobStore blobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final VerifyMediaProcessor< Tape, BlobTape, VerifyTask > processor = new VerifyMediaProcessor<>( 
                Tape.class,
                Require.not( Require.beanPropertyEqualsOneOf( 
                        Tape.STATE, TapeState.getStatesThatDisallowTapeLoadIntoDrive() ) ),
                BlobTape.class,
                VerifyTask.class,
                dbSupport.getServiceManager(), 
                blobStore, 
                100,
                64 );
        
        TestUtil.sleep( 250 );
        final List< MethodInvokeData > invocations = btih.getMethodInvokeData( methodVerify );
        assertFalse(invocations.isEmpty(), "Shoulda made invocations on blob store to verify tape.");

        assertNotNull(processor, "Don't prematurely gc me.");
        processor.shutdown();
    }
    
    
    @Test
    public void testEligiblePoolsForAutomaticVerificationAreVerified()
    {
        final Method methodVerify = ReflectUtil.getMethod( BlobStore.class, "verify" );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain storageDomain2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.updateBean( 
                storageDomain.setMaximumAutoVerificationFrequencyInDays( Integer.valueOf( 30 ) ), 
                StorageDomain.MAXIMUM_AUTO_VERIFICATION_FREQUENCY_IN_DAYS );
        final Pool pool0 = mockDaoDriver.createPool( PoolState.NORMAL );
        final Pool pool1 = mockDaoDriver.createPool( PoolState.NORMAL );
        final Pool pool2 = mockDaoDriver.createPool( PoolState.NORMAL );
        final Pool pool3 = mockDaoDriver.createPool( PoolState.NORMAL );
        final Pool pool4 = mockDaoDriver.createPool( PoolState.NORMAL );
        final Pool pool5 = mockDaoDriver.createPool( PoolState.LOST );
        final Pool pool6 = mockDaoDriver.createPool( PoolState.NORMAL );
        mockDaoDriver.createPool( PoolState.NORMAL );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), pool0.getPartitionId() );
        final StorageDomainMember sdm2 = mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain2.getId(), pool0.getPartitionId() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.putBlobOnPool( pool0.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnPool( pool1.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnPool( pool3.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnPool( pool4.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnPool( pool5.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnPool( pool6.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.updateBean( 
                pool0.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        mockDaoDriver.updateBean( 
                pool1.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        mockDaoDriver.updateBean( 
                pool2.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ).setLastVerified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED, PersistenceTarget.LAST_VERIFIED );
        mockDaoDriver.updateBean( 
                pool3.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        mockDaoDriver.updateBean( 
                pool4.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ).setLastVerified( new Date() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED, PersistenceTarget.LAST_VERIFIED );
        mockDaoDriver.updateBean( 
                pool5.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        mockDaoDriver.updateBean( 
                pool6.setStorageDomainMemberId( sdm2.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 1000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBlobStoreIh( 64, 0 ) );
        final PoolBlobStore blobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, btih );
        final VerifyMediaProcessor< Pool, BlobPool, VerifyTask > processor = new VerifyMediaProcessor<>( 
                Pool.class,
                Require.beanPropertyEquals( Pool.STATE, PoolState.NORMAL ),
                BlobPool.class,
                VerifyTask.class,
                dbSupport.getServiceManager(), 
                blobStore, 
                100,
                64 );
        
        List< MethodInvokeData > invocations = btih.getMethodInvokeData( methodVerify );
        int i = 20;
        while ( 0 < i && 2 > invocations.size() )
        {
            TestUtil.sleep( 100 );
            --i;
            invocations = btih.getMethodInvokeData( methodVerify );
        }
        assertTrue(1 < invocations.size(), "Shoulda made invocations on blob store to verify pool.");

        invocations = btih.getMethodInvokeData( methodVerify );
        i = 20;
        while ( 0 < i && 4 > invocations.size() )
        {
            TestUtil.sleep( 100 );
            --i;
            invocations = btih.getMethodInvokeData( methodVerify );
        }
        assertTrue(3 < invocations.size(), "Shoulda made invocations on blob store to verify pool.");

        assertNotNull(processor, "Don't prematurely gc me.");
        processor.shutdown();
    }
    
    
    @Test
    public void testEligiblePoolsForAutomaticVerificationAreNotVerifiedWhenTooManyTasksQueued()
    {
        final Method methodVerify = ReflectUtil.getMethod( BlobStore.class, "verify" );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        mockDaoDriver.updateBean( 
                storageDomain.setMaximumAutoVerificationFrequencyInDays( Integer.valueOf( 30 ) ), 
                StorageDomain.MAXIMUM_AUTO_VERIFICATION_FREQUENCY_IN_DAYS );
        final Pool pool0 = mockDaoDriver.createPool( PoolState.NORMAL );
        final Pool pool1 = mockDaoDriver.createPool( PoolState.NORMAL );
        final Pool pool2 = mockDaoDriver.createPool( PoolState.NORMAL );
        final Pool pool3 = mockDaoDriver.createPool( PoolState.NORMAL );
        final Pool pool4 = mockDaoDriver.createPool( PoolState.NORMAL );
        final Pool pool5 = mockDaoDriver.createPool( PoolState.LOST );
        mockDaoDriver.createPool( PoolState.NORMAL );
        final StorageDomainMember sdm1 = mockDaoDriver.addPoolPartitionToStorageDomain(
                storageDomain.getId(), pool0.getPartitionId() );
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.putBlobOnPool( pool0.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnPool( pool1.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnPool( pool2.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnPool( pool3.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnPool( pool4.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.putBlobOnPool( pool5.getId(), mockDaoDriver.getBlobFor( o1.getId() ).getId() );
        mockDaoDriver.updateBean( 
                pool0.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        mockDaoDriver.updateBean( 
                pool1.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        mockDaoDriver.updateBean( 
                pool2.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ).setLastVerified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED, PersistenceTarget.LAST_VERIFIED );
        mockDaoDriver.updateBean( 
                pool3.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        mockDaoDriver.updateBean( 
                pool4.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ).setLastVerified( new Date() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED, PersistenceTarget.LAST_VERIFIED );
        mockDaoDriver.updateBean( 
                pool5.setStorageDomainMemberId( sdm1.getId() ).setAssignedToStorageDomain( true )
                .setLastModified( new Date( 100000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                PersistenceTarget.LAST_MODIFIED );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( getBlobStoreIh( 4, 63 ) );
        final PoolBlobStore blobStore = InterfaceProxyFactory.getProxy( PoolBlobStore.class, btih );
        final VerifyMediaProcessor< Pool, BlobPool, VerifyTask > processor = new VerifyMediaProcessor<>( 
                Pool.class,
                Require.beanPropertyEquals( Pool.STATE, PoolState.NORMAL ),
                BlobPool.class,
                VerifyTask.class,
                dbSupport.getServiceManager(), 
                blobStore, 
                125,
                64 );
        
        TestUtil.sleep( 200 );
        final List< MethodInvokeData > invocations = btih.getMethodInvokeData( methodVerify );
        assertEquals( 1,  invocations.size(), "Shoulda made invocation on blob store to verify pool.");

        assertNotNull(processor, "Don't prematurely gc me.");
        processor.shutdown();
    }
    
    
    private interface VerifyTask extends BlobStoreTask
    {
        // empty
    } // end inner class def
    
    
    private InvocationHandler getBlobStoreIh(
            final int numOtherTasks, 
            final int numVerifyTasks )
    {
        final Set< BlobStoreTask > tasks = new HashSet<>();
        for ( int i = 0; i < numOtherTasks; ++i )
        {
            tasks.add( InterfaceProxyFactory.getProxy( BlobStoreTask.class, null ) );
        }
        for ( int i = 0; i < numVerifyTasks; ++i )
        {
            tasks.add( InterfaceProxyFactory.getProxy( VerifyTask.class, null ) );
        }
        
        final Method methodGetTasks = ReflectUtil.getMethod( BlobStore.class, "getTasks" );
        return MockInvocationHandler.forMethod( 
                methodGetTasks, 
                new InvocationHandler()
                {
                    @Override
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        return tasks;
                    }
                },
                null );
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
