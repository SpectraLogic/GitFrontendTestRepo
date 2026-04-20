/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeBlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeLockSupport;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.MockBeansServiceManager;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;


import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class ReclaimTapeProcessor_Test
{
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            @Override
            public void test()
            {
                new ReclaimTapeProcessor(
                        null,
                        InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ),
                        new Object(),
                        InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                        100 );
            }
        } );
    }
    
    @Test
    public void testConstructorNullBlobStoreNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            @Override
            public void test()
            {
                new ReclaimTapeProcessor(
                        new MockBeansServiceManager(), 
                        null,
                        new Object(),
                        InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                        100 );
            }
        } );
    }
    
    @Test
    public void testConstructorNullTaskStateLockNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            @Override
            public void test()
            {
                new ReclaimTapeProcessor(
                        new MockBeansServiceManager(), 
                        InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ),
                        null,
                        InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                        100 );
            }
        } );
    }
    
    @Test
    public void testConstructorNullTapeLockSupportNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            @Override
            public void test()
            {
                new ReclaimTapeProcessor(
                        new MockBeansServiceManager(), 
                        InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ),
                        new Object(),
                        null,
                        100 );
            }
        } );
    }
    
    @Test
    public void testTapesFormattedWhenEligibleForFormattingEvenIfVeryLittleCapacityUsedOnThem()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final UUID storageDomainId = storageDomain.getId();
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.EJECTED );
        final Tape tape3 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape4 = mockDaoDriver.createTape( TapeState.LOST );
        final Tape tape5 = mockDaoDriver.createTape( TapeState.EJECTED );
        final Tape tape6 = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomainId, tape1.getPartitionId(), tape1.getType() );
        
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape1.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape2.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape3.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape4.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape5.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape6.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true )
                     .setAvailableRawCapacity( Long.valueOf( 1000 ) )
                     .setTotalRawCapacity( Long.valueOf( 1001 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                Tape.AVAILABLE_RAW_CAPACITY, Tape.TOTAL_RAW_CAPACITY );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape3.getId(), b3.getId() );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final ReclaimTapeProcessor reclaimer = new ReclaimTapeProcessor(
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih ),
                new Object(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                200000 ); // Set worker start delay to absurd value to ensure
                          // it never starts this way during this test.
        
        reclaimer.run();
        assertNotNull(
                reclaimer,
                "Don't prematurely GC me."
                 );
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda notta formatted any tapes."
               );
        reclaimer.shutdown();
    }
    
    @Test
    public void testNoTapesFormattedIfNoneAreEligibleForFormatting()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final UUID storageDomainId = storageDomain.getId();
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.EJECTED );
        final Tape tape3 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape4 = mockDaoDriver.createTape( TapeState.LOST );
        final Tape tape5 = mockDaoDriver.createTape( TapeState.EJECTED );
        final Tape tape6 = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomainId, tape1.getPartitionId(), tape1.getType() );
        
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape1.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape2.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape3.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape4.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape5.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape6.setStorageDomainMemberId( sdm.getId() )
                .setAssignedToStorageDomain( true )
                .setWriteProtected( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                Tape.WRITE_PROTECTED );

        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape3.getId(), b3.getId() );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final ReclaimTapeProcessor reclaimer = new ReclaimTapeProcessor(
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih ),
                new Object(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                200000 ); // Set worker start delay to absurd value to ensure
                          // it never starts this way during this test.
        
        reclaimer.run();
        assertNotNull(
                reclaimer,
                "Don't prematurely GC me."
                 );
        assertEquals(
                0,
                btih.getTotalCallCount(),
                "Shoulda notta formatted any tapes."
                 );
        reclaimer.shutdown();
    }
    
    @Test
    public void testTapesFormattedAsTheyAreDeterminedToBeFormattable()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain storageDomain2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        mockDaoDriver.updateBean( 
                storageDomain2.setSecureMediaAllocation( true ),
                StorageDomain.SECURE_MEDIA_ALLOCATION );
        final UUID storageDomainId = storageDomain.getId();
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.EJECTED );
        final Tape tape3 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape4 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape5 = mockDaoDriver.createTape( TapeState.EJECTED );
        final Tape tape6 = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean( 
                tape6.setAvailableRawCapacity( null ).setTotalRawCapacity( null ), 
                Tape.AVAILABLE_RAW_CAPACITY, Tape.TOTAL_RAW_CAPACITY );
        mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomainId, tape1.getPartitionId(), tape1.getType() );
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain2.getId(), tape1.getPartitionId(), tape1.getType() );
        
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape1.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape2.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape3.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape4.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape5.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape6.setStorageDomainMemberId( sdm2.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2" );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final Blob b3 = mockDaoDriver.getBlobFor( o3.getId() );
        mockDaoDriver.putBlobOnTape( tape1.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape3.getId(), b3.getId() );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final ReclaimTapeProcessor reclaimer = new ReclaimTapeProcessor(
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih ),
                new Object(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                200000 ); // Set worker start delay to absurd value to ensure
                          // it never starts this way during this test.
        
        reclaimer.run();
        assertEquals(
                1,
                btih.getTotalCallCount(),
                "Shoulda called format for tape 4."
                );
        
        final Method method = ReflectUtil.getMethod( TapeBlobStore.class, "formatTape" );
        assertEquals(
                1,
                btih.getMethodCallCount( method ),
                "Shoulda formatted tape 4."
                 );
        assertEquals(
                tape4.getId(),
                btih.getMethodInvokeData( method ).get( 0 ).getArgs().get( 1 ),
                "Shoulda formatted tape 4."
                );
        mockDaoDriver.delete( Tape.class, tape4 );

        mockDaoDriver.updateBean(
                tape6.setAvailableRawCapacity( Long.valueOf( 99 ) )
                      .setTotalRawCapacity( Long.valueOf( 100 ) ), 
                Tape.AVAILABLE_RAW_CAPACITY, Tape.TOTAL_RAW_CAPACITY );
        
        reclaimer.run();
        assertEquals(
                1,
                btih.getMethodCallCount( method ),
                "Should notta formatted another tape yet."
                );

        mockDaoDriver.updateBean(
                tape6.setAvailableRawCapacity( Long.valueOf( 95 ) )
                      .setTotalRawCapacity( Long.valueOf( 100 ) ), 
                Tape.AVAILABLE_RAW_CAPACITY, Tape.TOTAL_RAW_CAPACITY );
        
        reclaimer.run();
        assertEquals(
                2,
                btih.getMethodCallCount( method ),
                "Shoulda formatted tape 6."
                 );
        assertEquals(
                tape6.getId(),
                btih.getMethodInvokeData( method ).get( 1 ).getArgs().get( 1 ),
                "Shoulda formatted tape 6."
                );
        reclaimer.shutdown();
    }
    
    @Test
    public void testTapeLocksDoNotStopReclaimerFromWorking()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final UUID storageDomainId = storageDomain.getId();
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.EJECTED );
        final Tape tape3 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape4 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape5 = mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomainId, tape1.getPartitionId(), tape1.getType() );
        
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape1.setAssignedToStorageDomain( true ).setAvailableRawCapacity( Long.valueOf( 1 ) )
                .setTotalRawCapacity( Long.valueOf( 100 ) ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                Tape.AVAILABLE_RAW_CAPACITY, Tape.TOTAL_RAW_CAPACITY );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape2.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape3.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true )
                .setAvailableRawCapacity( Long.valueOf( 100 ) ).setTotalRawCapacity( Long.valueOf( 1000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                Tape.AVAILABLE_RAW_CAPACITY, Tape.TOTAL_RAW_CAPACITY );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape4.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape5.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        
        final AtomicBoolean locked = new AtomicBoolean( true );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final ReclaimTapeProcessor reclaimer = new ReclaimTapeProcessor(
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih ),
                new Object(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, new InvocationHandler()
                {
                    @Override
                    public Object invoke( final Object proxy, final Method method, final Object[] args ) 
                            throws Throwable
                    {
                        if ( locked.get() )
                        {
                            return new Object();
                        }
                        return null;
                    }
                } ),
                200000 ); // Set worker start delay to absurd value to ensure
                          // it never starts this way during this test.
        
        reclaimer.run();
        assertEquals(
                0,
                btih.getTotalCallCount(),
                "Should notta called format for tapes 1, 3, and 4."
               );
        
        locked.set( false );
        reclaimer.run();
        assertTrue(
                3 <= btih.getTotalCallCount(),
                "Shoulda called format for tapes 1, 3, and 4."
                );
        assertNotNull(
                reclaimer,
                "Don't prematurely GC me."
                 );
        reclaimer.shutdown();
    }
    
    @Test
    public void testFormatExceptionsDoNotStopReclaimerFromWorking()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final UUID storageDomainId = storageDomain.getId();
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.EJECTED );
        final Tape tape3 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape4 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape5 = mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomainId, tape1.getPartitionId(), tape1.getType() );
        
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape1.setAssignedToStorageDomain( true ).setAvailableRawCapacity( Long.valueOf( 1 ) )
                .setTotalRawCapacity( Long.valueOf( 100 ) ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, 
                Tape.AVAILABLE_RAW_CAPACITY, Tape.TOTAL_RAW_CAPACITY );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape2.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape3.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true )
                .setAvailableRawCapacity( Long.valueOf( 100 ) ).setTotalRawCapacity( Long.valueOf( 1000 ) ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                Tape.AVAILABLE_RAW_CAPACITY, Tape.TOTAL_RAW_CAPACITY );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape4.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape5.setStorageDomainMemberId( sdm.getId() ).setAssignedToStorageDomain( true ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            @Override
            public Object invoke( final Object proxy, final Method method, final Object[] args )
                    throws Throwable
            {
                if ( tape3.getId().equals( args[ 1 ] ) )
                {
                    throw new RuntimeException( "Uh uh uh...  You can't format THAT tape." );
                }
                return Boolean.TRUE;
            }
        } );
        final ReclaimTapeProcessor reclaimer = new ReclaimTapeProcessor(
                dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih ),
                new Object(),
                InterfaceProxyFactory.getProxy( TapeLockSupport.class, null ),
                200000 ); // Set worker start delay to absurd value to ensure
                          // it never starts this way during this test.
        
        reclaimer.run();
        assertEquals(
                3,
                btih.getTotalCallCount(),
                "Shoulda called format for tapes 1, 3, and 4."
                );
        
        reclaimer.run();
        assertTrue(
                3 < btih.getTotalCallCount(),
                "Shoulda retried format for tapes 1, 3, and 4 AGAIN."
                 );
        assertNotNull(
                reclaimer,
                "Don't prematurely GC me."
                 );
        reclaimer.shutdown();
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
