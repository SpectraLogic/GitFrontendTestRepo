/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape;

import java.lang.reflect.InvocationHandler;
import java.util.HashSet;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveState;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionState;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapePartitionService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeLockSupport;
import com.spectralogic.s3.dataplanner.backend.tape.task.MockTapeDriveResource;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.client.RpcClient;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class TapeLockSupportImpl_Test
{

    @Test
    public void testConstructorNullRpcClientNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new TapeLockSupportImpl<>(
                        null,
                        dbSupport.getServiceManager() );
            }
        } );
    }
    
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new TapeLockSupportImpl<>(
                        InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                        null );
            }
        } );
    }
    
    @Test
    public void testLockForNonExistantTapeDriveNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );
        
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        support.ensureAvailableTapeDrivesAreUpToDate();
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "2" );

        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        TestUtil.assertThrows( null, UnsupportedOperationException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( drive2.getId(), Integer.valueOf( 1 ) );
            }
        } );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( UUID.randomUUID(), Integer.valueOf( 3 ) );
            }
        } );
    }
    
    @Test
    public void testLockForExistantTapeDriveWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );
        
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "2" );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( null, "3" );
        mockDaoDriver.createTapeDrive( null, "4" );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        support.lock( drive2.getId(), Integer.valueOf( 2 ) );
        support.lock( drive3.getId(), Integer.valueOf( 3 ) );

        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( drive1.getId(), Integer.valueOf( 4 ) );
            }
        } );
        
        support.unlock( Integer.valueOf( 1 ) );
        support.lock( drive1.getId(), Integer.valueOf( 4 ) );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( drive3.getId(), Integer.valueOf( 5 ) );
            }
        } );
    }
    
    @Test
    public void testLockTapeDrivelesslyInPartitionWithErrorNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final Tape tape = mockDaoDriver.createTape();
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapePartition partition =
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain( 
                        drive1.getPartitionId() );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition.setState( TapePartitionState.ERROR ), TapePartition.STATE );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lockWithoutDrive( Integer.valueOf( 2 ) );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.addTapeLock( Integer.valueOf( 2 ), tape.getId() );
            }
        } );
    }
    
    @Test
    public void testLockTapeDrivelesslyWhenBackendDeactivatedNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        mockDaoDriver.updateAllBeans( 
                BeanFactory.newBean( DataPathBackend.class ).setActivated( false ),
                DataPathBackend.ACTIVATED );
        final Tape tape = mockDaoDriver.createTape();
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapePartition partition =
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain( 
                        drive1.getPartitionId() );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition.setQuiesced( Quiesced.NO ), TapePartition.QUIESCED );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lockWithoutDrive( Integer.valueOf( 2 ) );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.addTapeLock( Integer.valueOf( 2 ), tape.getId() );
            }
        } );
    }
    
    @Test
    public void testLockTapeDrivelesslyInPartitionQuiescedNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );
        
        final Tape tape = mockDaoDriver.createTape();
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapePartition partition =
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain( 
                        drive1.getPartitionId() );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition.setQuiesced( Quiesced.YES ), TapePartition.QUIESCED );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lockWithoutDrive( Integer.valueOf( 2 ) );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.addTapeLock( Integer.valueOf( 2 ), tape.getId() );
            }
        } );
    }
    
    @Test
    public void testLockTapeDriveInPartitionWithErrorNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );
        
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapePartition partition =
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain( 
                        drive1.getPartitionId() );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition.setState( TapePartitionState.ERROR ), TapePartition.STATE );
        support.ensureAvailableTapeDrivesAreUpToDate();

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( drive1.getId(), Integer.valueOf( 2 ) );
            }
        } );
    }
    
    @Test
    public void testLockTapeDriveInPartitionQuiescedNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );
        
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapePartition partition =
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain( 
                        drive1.getPartitionId() );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition.setQuiesced( Quiesced.YES ), TapePartition.QUIESCED );
        support.ensureAvailableTapeDrivesAreUpToDate();

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( drive1.getId(), Integer.valueOf( 2 ) );
            }
        } );
    }
    
    @Test
    public void testLockQuiescedOrPendingQuiesceTapeDriveNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );
        
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        support.lock( drive1.getId(), Integer.valueOf( 2 ) );
        support.unlock( Integer.valueOf( 2 ) );
        
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive1.setQuiesced( Quiesced.PENDING ), TapeDrive.QUIESCED );
        support.ensureAvailableTapeDrivesAreUpToDate();

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( drive1.getId(), Integer.valueOf( 2 ) );
            }
        } );
        
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive1.setQuiesced( Quiesced.YES ), TapeDrive.QUIESCED );
        support.ensureAvailableTapeDrivesAreUpToDate();

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( drive1.getId(), Integer.valueOf( 2 ) );
            }
        } );
    }
    
    @Test
    public void testLockTapeDriveWhenBackendDeactivatedNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        mockDaoDriver.updateAllBeans( 
                BeanFactory.newBean( DataPathBackend.class ).setActivated( false ),
                DataPathBackend.ACTIVATED );
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapePartition partition =
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain( 
                        drive1.getPartitionId() );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition.setQuiesced( Quiesced.NO ), TapePartition.QUIESCED );
        support.ensureAvailableTapeDrivesAreUpToDate();

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( drive1.getId(), Integer.valueOf( 2 ) );
            }
        } );
    }
    
    @Test
    public void testLockTapeDriveInErrorNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );
        
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive1.setState( TapeDriveState.ERROR ), TapeDrive.STATE );
        support.ensureAvailableTapeDrivesAreUpToDate();

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( drive1.getId(), Integer.valueOf( 2 ) );
            }
        } );
    }
    
    @Test
    public void testRefreshTapeDrivesWhileDrivesAreLockedWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "2" );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( null, "3" );
        mockDaoDriver.createTapeDrive( null, "4" );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        support.lock( drive2.getId(), Integer.valueOf( 2 ) );
        support.lock( drive3.getId(), Integer.valueOf( 3 ) );
        
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive1.setState( TapeDriveState.ERROR ), TapeDrive.STATE );
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive2.setState( TapeDriveState.ERROR ), TapeDrive.STATE );
        mockDaoDriver.createTapeDrive( null, "5" );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.unlock( Integer.valueOf( 1 ) );
        
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive1.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive2.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lock( drive1.getId(), Integer.valueOf( 98 ) );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                support.lock( drive2.getId(), Integer.valueOf( 99 ) );
            }
        } );
        
        support.unlock( Integer.valueOf( 2 ) );
        support.unlock( Integer.valueOf( 3 ) );
    }
    
    @Test
    public void testRefreshTapeDrivesWhenPartitionGoesOfflineWhileDrivesAreLockedWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "2" );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( null, "3" );
        mockDaoDriver.createTapeDrive( null, "4" );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        support.lock( drive2.getId(), Integer.valueOf( 2 ) );
        support.lock( drive3.getId(), Integer.valueOf( 3 ) );
        
        dbSupport.getDataManager().updateBeans( 
                CollectionFactory.toSet( TapePartition.STATE ),
                BeanFactory.newBean( TapePartition.class ).setState( TapePartitionState.OFFLINE ),
                Require.nothing() );
        mockDaoDriver.createTapeDrive( null, "5" );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.unlock( Integer.valueOf( 1 ) );
        support.unlock( Integer.valueOf( 2 ) );
        support.unlock( Integer.valueOf( 3 ) );
    }
    
    @Test
    public void testRefreshTapeDrivesWhenPartitionGoesQuiescedWhileDrivesAreLockedWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "2" );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( null, "3" );
        mockDaoDriver.createTapeDrive( null, "4" );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        support.lock( drive2.getId(), Integer.valueOf( 2 ) );
        support.lock( drive3.getId(), Integer.valueOf( 3 ) );
        
        dbSupport.getDataManager().updateBeans( 
                CollectionFactory.toSet( TapePartition.QUIESCED ),
                BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.PENDING ),
                Require.nothing() );
        mockDaoDriver.createTapeDrive( null, "5" );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.unlock( Integer.valueOf( 1 ) );
        support.unlock( Integer.valueOf( 2 ) );
        support.unlock( Integer.valueOf( 3 ) );
    }
    
    @Test
    public void testRefreshTapeDrivesWhenDrivesLockedAndPendingQuiesceWorks()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "2" );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( null, "3" );
        mockDaoDriver.createTapeDrive( null, "4" );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        support.lock( drive2.getId(), Integer.valueOf( 2 ) );
        support.lock( drive3.getId(), Integer.valueOf( 3 ) );
        
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive1.setQuiesced( Quiesced.PENDING ), TapeDrive.QUIESCED );
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive2.setQuiesced( Quiesced.PENDING ), TapeDrive.QUIESCED );
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive3.setQuiesced( Quiesced.PENDING ), TapeDrive.QUIESCED );
        mockDaoDriver.createTapeDrive( null, "5" );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.unlock( Integer.valueOf( 1 ) );
        support.unlock( Integer.valueOf( 2 ) );
        support.unlock( Integer.valueOf( 3 ) );
    }
    
    @Test
    public void testDrivelessLocksWork()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "2" );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( null, "3" );
        mockDaoDriver.createTapeDrive( null, "4" );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        support.lock( drive2.getId(), Integer.valueOf( 2 ) );
        support.lockWithoutDrive( Integer.valueOf( 3 ) );
        TestUtil.assertThrows( null, UnsupportedOperationException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( drive3.getId(), Integer.valueOf( 3 ) );
            }
        } );
        
        support.lock( drive3.getId(), Integer.valueOf( 4 ) );
        
        support.unlock( Integer.valueOf( 3 ) );
        
        support.lockWithoutDrive( Integer.valueOf( 5 ) );
        support.lockWithoutDrive( Integer.valueOf( 6 ) );
        support.unlock( Integer.valueOf( 5 ) );
        support.unlock( Integer.valueOf( 6 ) );
    }
    
    @Test
    public void testTapeIdLocksTakenAndReportedCorrectly()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "2" );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( null, "3" );
        final TapeDrive drive4 = mockDaoDriver.createTapeDrive( null, "4" );
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                CollectionFactory.toSet( drive1.getId(), drive2.getId(), drive3.getId(), drive4.getId() ),
                support.getAvailableTapeDrives(),
                "Shoulda reported correct set of available drives."
                 );

        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        support.lock( drive2.getId(), Integer.valueOf( 2 ) );
        support.lockWithoutDrive( Integer.valueOf( 3 ) );
        TestUtil.assertThrows( null, UnsupportedOperationException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( drive3.getId(), Integer.valueOf( 3 ) );
            }
        } );
        assertEquals(
                CollectionFactory.toSet( drive3.getId(), drive4.getId() ),
                support.getAvailableTapeDrives(),
                "Shoulda reported correct set of available drives."
                );
        
        support.lock( drive3.getId(), Integer.valueOf( 4 ) );
        
        support.unlock( Integer.valueOf( 3 ) );
        
        support.lockWithoutDrive( Integer.valueOf( 5 ) );
        support.lockWithoutDrive( Integer.valueOf( 6 ) );
        
        final UUID tapeId1 = UUID.randomUUID();
        final UUID tapeId2 = UUID.randomUUID();
        support.addTapeLock( Integer.valueOf( 1 ), tapeId1 );
        support.addTapeLock( Integer.valueOf( 5 ), tapeId2 );
        assertEquals(
                CollectionFactory.toSet( tapeId1, tapeId2 ),
                support.getLockedTapes( null ),
                "Shoulda reported 2 tape ids as being locked."
                 );
        assertEquals(
                CollectionFactory.toSet( tapeId1 ),
                support.getLockedTapes( Integer.valueOf( 1 ) ),
                "Shoulda reported tape locked by lock holder."
                );
        assertEquals(
                CollectionFactory.toSet( tapeId2 ),
                support.getLockedTapes( Integer.valueOf( 5 ) ),
                "Shoulda reported tape locked by lock holder."
                );
        assertEquals(
                new HashSet<>(),
                support.getLockedTapes( Integer.valueOf( 2 ) ),
                "Shoulda reported tape locked by lock holder."
                 );
        
        support.unlock( Integer.valueOf( 5 ) );
        assertEquals(
                CollectionFactory.toSet( tapeId1 ),
                support.getLockedTapes( null ),
                "Shoulda reported 1 tape id as being locked."
                 );
        support.lockWithoutDrive( Integer.valueOf( 9 ) );
        assertEquals(
                CollectionFactory.toSet( tapeId1 ),
                support.getLockedTapes( null ),
                "Shoulda reported 1 tape id as being locked."
                 );
        
        support.addTapeLock( Integer.valueOf( 9 ), tapeId2 );
        assertEquals(
                CollectionFactory.toSet( tapeId1, tapeId2 ),
                support.getLockedTapes( null ),
                "Shoulda reported 2 tape ids as being locked."
                 );
        
        assertEquals(
                CollectionFactory.toSet( Integer.valueOf( 1 ), Integer.valueOf( 2 ), Integer.valueOf( 4 ),
                        Integer.valueOf( 6 ), Integer.valueOf( 9 ) ),
                support.getAllLockHolders(),
                "Shoulda reported correct lock holders set."
                 );
        
        assertEquals(
                null,
                support.getTapeLockHolder( UUID.randomUUID() ),
                "Shoulda reported no tape lock holder."
                );
        assertEquals(
                CollectionFactory.toSet( drive4.getId() ),
                support.getAvailableTapeDrives(),
                "Shoulda reported correct set of available drives."
                 );
        
        support.unlock( Integer.valueOf( 9 ) );
        assertEquals(
                CollectionFactory.toSet( drive4.getId() ),
                support.getAvailableTapeDrives(),
                "Shoulda reported correct set of available drives."
                 );
        
        support.unlock( Integer.valueOf( 6 ) );
        assertEquals(
                CollectionFactory.toSet( drive4.getId() ),
                support.getAvailableTapeDrives(),
                "Shoulda reported correct set of available drives."
                 );
        
        support.unlock( Integer.valueOf( 2 ) );
        assertEquals(
                CollectionFactory.toSet( drive2.getId(), drive4.getId() ),
                support.getAvailableTapeDrives(),
                "Shoulda reported correct set of available drives."
                 );
        assertEquals(
                CollectionFactory.toSet( Integer.valueOf( 1 ), Integer.valueOf( 4 ) ),
                support.getAllLockHolders(),
                "Shoulda reported correct lock holders set."
                );
    }
    
    @Test
    public void testUnlockWhenNoLockTakenNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        mockDaoDriver.createTapeDrive( null, "4" );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lock( drive1.getId(), Integer.valueOf( 1 ) );

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.unlock( Integer.valueOf( 2 ) );
            }
        } );
    }
    
    @Test
    public void testLockSameTapeTwiceNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "2" );
        mockDaoDriver.createTapeDrive( null, "4" );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        support.lock( drive2.getId(), Integer.valueOf( 2 ) );
        
        final UUID tapeId1 = UUID.randomUUID();
        support.addTapeLock( Integer.valueOf( 1 ), tapeId1 );

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.addTapeLock( Integer.valueOf( 2 ), tapeId1 );
            }
        } );
    }
    
    @Test
    public void testPartitionQuiescedStateUpdatedToYesFromPendingEvenIfDriveInitializedToQuiesceRequired()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "a" );
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, "b" );
        mockDaoDriver.createTapeDrive( partition1.getId(), "1" );
        mockDaoDriver.createTapeDrive( partition1.getId(), "2" );
        mockDaoDriver.createTapeDrive( partition2.getId(), "3" );
        final TapeDrive drive4 = mockDaoDriver.createTapeDrive( 
                partition2.getId(), "4", mockDaoDriver.createTape().getId() );

        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition1.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition2.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                Quiesced.YES,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition1.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition2.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( drive4.getId(), Integer.valueOf( 2 ) );
            }
        } );
        support.forceLock( drive4.getId(), Integer.valueOf( 2 ) );
        mockDaoDriver.updateBean( drive4.setTapeId( null ), TapeDrive.TAPE_ID );
        support.unlock( Integer.valueOf( 2 ) );
    }
    
    @Test
    public void testPartitionQuiescedStateUpdatedToYesFromPendingOnceNoDriveLocksOutstanding()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "a" );
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, "b" );
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( partition1.getId(), "1" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( partition1.getId(), "2" );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( partition2.getId(), "3" );
        final TapeDrive drive4 = mockDaoDriver.createTapeDrive(
                partition2.getId(), "4", mockDaoDriver.createTape().getId() );
        support.ensureAvailableTapeDrivesAreUpToDate();
        
        assertEquals(
                Quiesced.NO,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition1.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
        assertEquals(
                Quiesced.NO,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition2.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );

        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        support.lock( drive2.getId(), Integer.valueOf( 2 ) );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition1.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition2.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition1.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition2.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
        
        mockDaoDriver.updateBean( drive4.setTapeId( null ), TapeDrive.TAPE_ID );
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition1.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
        assertEquals(
                Quiesced.YES,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition2.getId() ).getQuiesced(),
                "Shoulda changed quiesced state."
                 );
        
        support.unlock( Integer.valueOf( 1 ) );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition1.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                );
        assertEquals(
                Quiesced.YES,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition2.getId() ).getQuiesced(),
                "Shoulda changed quiesced state."
                 );
        
        support.unlock( Integer.valueOf( 2 ) );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                Quiesced.YES,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition1.getId() ).getQuiesced(),
                "Shoulda changed quiesced state."
                 );
        assertEquals(
                Quiesced.YES,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition2.getId() ).getQuiesced(),
                "Shoulda changed quiesced state."
                 );

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( drive1.getId(), Integer.valueOf( 2 ) );
            }
        } );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.lock( drive3.getId(), Integer.valueOf( 2 ) );
            }
        } );
        
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition1.setQuiesced( Quiesced.NO ), TapePartition.QUIESCED );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition2.setQuiesced( Quiesced.NO ), TapePartition.QUIESCED );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        
        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        support.lock( drive2.getId(), Integer.valueOf( 2 ) );
        support.lock( drive3.getId(), Integer.valueOf( 3 ) );
        support.lock( drive4.getId(), Integer.valueOf( 4 ) );
        
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition1.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition2.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition1.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition2.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                );
    }
    
    @Test
    public void testDrivesQuiescedStateUpdatedToYesFromPendingOnceNoDriveLocksOutstanding()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final TapePartition partition = mockDaoDriver.createTapePartition( null, "a" );
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( partition.getId(), "1" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( partition.getId(), "2" );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( partition.getId(), "3" );
        final TapeDrive drive4 = mockDaoDriver.createTapeDrive(
                partition.getId(), "4", mockDaoDriver.createTape().getId() );
        
        support.ensureAvailableTapeDrivesAreUpToDate();        
        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        support.lock( drive2.getId(), Integer.valueOf( 2 ) );
        support.lock( drive3.getId(), Integer.valueOf( 3 ) );
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive1.setQuiesced( Quiesced.PENDING ), TapeDrive.QUIESCED );
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive2.setQuiesced( Quiesced.PENDING ), TapeDrive.QUIESCED );
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive4.setQuiesced( Quiesced.PENDING ), TapeDrive.QUIESCED );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain(
                        drive1.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain(
                        drive2.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
        assertEquals(
                Quiesced.NO,
                dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain(
                        drive3.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain(
                        drive4.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
                
        mockDaoDriver.updateBean( drive4.setTapeId( null ), TapeDrive.TAPE_ID );
        support.ensureAvailableTapeDrivesAreUpToDate();
        
        assertEquals(
                Quiesced.YES,
                dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain(
                        drive4.getId() ).getQuiesced(),
                "Shoulda changed quiesced state."
                 );
        
        support.unlock( Integer.valueOf( 1 ) );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        
        assertEquals(
                Quiesced.YES,
                dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain(
                        drive1.getId() ).getQuiesced(),
                "Shoulda changed quiesced state."
                );
                        
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain(
                        drive2.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
        
        support.unlock( Integer.valueOf( 2 ) );
        support.unlock( Integer.valueOf( 3 ) );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        
        assertEquals(
                Quiesced.YES,
                dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain(
                        drive2.getId() ).getQuiesced(),
                "Shoulda changed quiesced state."
                );
        assertEquals(
                Quiesced.NO,
                dbSupport.getServiceManager().getRetriever( TapeDrive.class ).attain(
                        drive3.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                );
    }
    
    @Test
    public void testTapePartitionFailureCreatedForQuiescedTapeDrive()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );
        final BeansRetriever< TapePartitionFailure > failureRetriever =
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class );
                
        final TapePartition partition = mockDaoDriver.createTapePartition( null, "a" );
        final TapeDrive drive1 = mockDaoDriver.createTapeDrive(
                partition.getId(),
                "1",
                mockDaoDriver.createTape(partition.getId(), TapeState.NORMAL ).getId() );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive(
                partition.getId(), "2", null);
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive2.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        dbSupport.getServiceManager().getService( TapeDriveService.class ).update(
                drive1.setQuiesced( Quiesced.PENDING ), TapeDrive.QUIESCED );
        support.ensureAvailableTapeDrivesAreUpToDate();
        
        assertEquals(
                0,
                failureRetriever.getCount(),
                "Should notta generated warning for drive pending quiesce."
                 );
        
        mockDaoDriver.updateBean( drive1.setTapeId( null ), TapeDrive.TAPE_ID );
        support.ensureAvailableTapeDrivesAreUpToDate();

        assertEquals(
                1,
                failureRetriever.getCount(),
                "Shoulda generated warning for quiesced tape drive."
                 );
        assertTrue(
                failureRetriever.attain( Require.nothing() )
                        .getType().equals( TapePartitionFailureType.TAPE_DRIVE_QUIESCED),
                                "Shoulda generated warning for quiesced tape drive."
                 );
    }
    
    @Test
    public void testPartitionQuiescedStateUpdatedToYesFromPendingOnceNoDrivelessLocksOutstanding()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, "a" );
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, "b" );
        final Tape tape1 = mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( partition1.getId(), TapeState.NORMAL );
        final Tape tape3 = mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL );
        final Tape tape4 = mockDaoDriver.createTape( partition2.getId(), TapeState.NORMAL );
        support.ensureAvailableTapeDrivesAreUpToDate();
        
        assertEquals(
                Quiesced.NO,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition1.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                );
        assertEquals(
                Quiesced.NO,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition2.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );

        support.lockWithoutDrive( Integer.valueOf( 1 ) );
        support.addTapeLock( Integer.valueOf( 1 ), tape1.getId() );
        support.lockWithoutDrive( Integer.valueOf( 2 ) );
        support.addTapeLock( Integer.valueOf( 2 ), tape2.getId() );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition1.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition2.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition1.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
        assertEquals(
                Quiesced.YES,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition2.getId() ).getQuiesced(),
                "Shoulda changed quiesced state."
                 );
        
        support.unlock( Integer.valueOf( 1 ) );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition1.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
        assertEquals(
                Quiesced.YES,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition2.getId() ).getQuiesced(),
                "Shoulda changed quiesced state."
                 );
        
        support.unlock( Integer.valueOf( 2 ) );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                Quiesced.YES,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition1.getId() ).getQuiesced(),
                "Shoulda changed quiesced state."
                 );
        assertEquals(
                Quiesced.YES,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition2.getId() ).getQuiesced(),
                "Shoulda changed quiesced state."
                 );

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.lockWithoutDrive( Integer.valueOf( 1 ) );
                support.addTapeLock( Integer.valueOf( 1 ), tape1.getId() );
            }
        } );
        support.unlock( Integer.valueOf( 1 ) );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.lockWithoutDrive( Integer.valueOf( 1 ) );
                support.addTapeLock( Integer.valueOf( 1 ), tape3.getId() );
            }
        } );
        support.unlock( Integer.valueOf( 1 ) );
        
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition1.setQuiesced( Quiesced.NO ), TapePartition.QUIESCED );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition2.setQuiesced( Quiesced.NO ), TapePartition.QUIESCED );
        
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lockWithoutDrive( Integer.valueOf( 1 ) );
        support.addTapeLock( Integer.valueOf( 1 ), tape1.getId() );
        support.lockWithoutDrive( Integer.valueOf( 2 ) );
        support.addTapeLock( Integer.valueOf( 2 ), tape2.getId() );
        support.lockWithoutDrive( Integer.valueOf( 3 ) );
        support.addTapeLock( Integer.valueOf( 3 ), tape3.getId() );
        support.lockWithoutDrive( Integer.valueOf( 4 ) );
        support.addTapeLock( Integer.valueOf( 4 ), tape4.getId() );
        
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition1.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );
        dbSupport.getServiceManager().getService( TapePartitionService.class ).update(
                partition2.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition1.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
        assertEquals(
                Quiesced.PENDING,
                dbSupport.getServiceManager().getRetriever( TapePartition.class ).attain(
                        partition2.getId() ).getQuiesced(),
                "Should notta changed quiesced state."
                 );
    }
    
    @Test
    public void testChangeTapeLockAfterLockTakenNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "2" );
        mockDaoDriver.createTapeDrive( null, "4" );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        support.lock( drive2.getId(), Integer.valueOf( 2 ) );
        
        final UUID tapeId1 = UUID.randomUUID();
        final UUID tapeId2 = UUID.randomUUID();
        support.addTapeLock( Integer.valueOf( 1 ), tapeId1 );
        support.addTapeLock( Integer.valueOf( 1 ), tapeId1 );

        TestUtil.assertThrows( null, UnsupportedOperationException.class, new BlastContainer()
        {
            public void test()
            {
                support.addTapeLock( Integer.valueOf( 1 ), tapeId2 );
            }
        } );
    }
    
    @Test
    public void testTakeNullTapeLockNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( null, "1" );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( null, "2" );
        mockDaoDriver.createTapeDrive( null, "4" );
        support.ensureAvailableTapeDrivesAreUpToDate();

        support.lock( drive1.getId(), Integer.valueOf( 1 ) );
        support.lock( drive2.getId(), Integer.valueOf( 2 ) );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                support.addTapeLock( Integer.valueOf( 1 ), null );
            }
        } );
    }
    
    @Test
    public void testLockTapeBeforeAcquiringLockNotAllowed()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );

        mockDaoDriver.createTapeDrive( null, "4" );
        support.ensureAvailableTapeDrivesAreUpToDate();
        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                support.addTapeLock( Integer.valueOf( 1 ), UUID.randomUUID() );
            }
        } );
    }
    
    @Test
    public void testTapePartitionFailureCreatedForOfflineStatePartition()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );
        
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, null );
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.updateBean( partition2.setState( TapePartitionState.OFFLINE ), TapePartition.STATE );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( partition1.getId(), null );
        mockDaoDriver.updateBean( drive1.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( partition2.getId(), null );
        mockDaoDriver.updateBean( drive2.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( partition2.getId(), null );
        mockDaoDriver.updateBean( drive3.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        
        final BeansRetriever< TapePartitionFailure > failureRetriever =
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class );
        assertEquals(
                1,
                failureRetriever.getCount(),
                "Shoulda generated failure for non-normal partition."
                 );
        assertTrue(
                failureRetriever.attain( Require.nothing() ).getErrorMessage().contains(
                        TapePartitionState.OFFLINE.toString() ),
                "Shoulda generated failure for non-normal partition."
                 );
    }
    
    @Test
    public void testTapePartitionFailureCreatedForErrorStatePartition()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );
        
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, null );
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.updateBean( partition2.setState( TapePartitionState.ERROR ), TapePartition.STATE );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( partition1.getId(), null );
        mockDaoDriver.updateBean( drive1.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( partition2.getId(), null );
        mockDaoDriver.updateBean( drive2.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( partition2.getId(), null );
        mockDaoDriver.updateBean( drive3.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        
        final BeansRetriever< TapePartitionFailure > failureRetriever =
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class );
        assertEquals(
                1,
                failureRetriever.getCount(),
                "Shoulda generated failure for non-normal partition."
                );
        assertTrue(
                failureRetriever.attain( Require.nothing() ).getErrorMessage().contains(
                        TapePartitionState.ERROR.toString() ),
                "Shoulda generated failure for non-normal partition."
                 );
    }
    
    @Test
    public void testTapePartitionFailureCreatedForQuiescePendingPartition()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );
        
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, null );
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.updateBean(
                partition2.setQuiesced( Quiesced.PENDING ), TapePartition.QUIESCED );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( partition1.getId(), null );
        mockDaoDriver.updateBean( drive1.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( partition2.getId(), null );
        mockDaoDriver.updateBean( drive2.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( partition2.getId(), null );
        mockDaoDriver.updateBean( drive3.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        
        final BeansRetriever< TapePartitionFailure > failureRetriever =
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class );
        assertEquals(
                1,
                failureRetriever.getCount(),
                "Shoulda generated failure for non-normal partition."
                );
        assertTrue(
                failureRetriever.attain( Require.nothing() ).getErrorMessage().contains(
                        Quiesced.PENDING.toString() ),
                "Shoulda generated failure for non-normal partition."
                 );
    }
    
    @Test
    public void testTapePartitionFailureCreatedForQuiescedPartition()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );
        
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, null );
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, null );
        mockDaoDriver.updateBean(
                partition2.setQuiesced( Quiesced.YES ), TapePartition.QUIESCED );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( partition1.getId(), null );
        mockDaoDriver.updateBean( drive1.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( partition2.getId(), null );
        mockDaoDriver.updateBean( drive2.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( partition2.getId(), null );
        mockDaoDriver.updateBean( drive3.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        
        final BeansRetriever< TapePartitionFailure > failureRetriever =
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class );
        assertEquals(
                1,
                failureRetriever.getCount(),
                "Shoulda generated failure for non-normal partition."
                 );
        assertTrue(
                failureRetriever.attain( Require.nothing() ).getErrorMessage().contains(
                        Quiesced.YES.toString() ),
                "Shoulda generated failure for non-normal partition."
                 );
    }
    
    @Test
    public void testTapePartitionFailureCreatedForErredDrives()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );
        
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, null );
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, null );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( partition1.getId(), null );
        mockDaoDriver.updateBean( drive1.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( partition2.getId(), null );
        mockDaoDriver.updateBean( drive2.setState( TapeDriveState.ERROR ), TapeDrive.STATE );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( partition2.getId(), null );
        mockDaoDriver.updateBean( drive3.setState( TapeDriveState.OFFLINE ), TapeDrive.STATE );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        
        final BeansRetriever< TapePartitionFailure > failureRetriever =
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class );
        assertEquals(
                1,
                failureRetriever.getCount(),
                "Shoulda generated failure for erred drives."
                 );
    }
    
    @Test
    public void testTapePartitionFailureCreatedAndDeletedAsFailureConditionComesAndGoes()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final TapeLockSupport< Integer > support = new TapeLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( RpcClient.class, getRpcClientIh() ),
                dbSupport.getServiceManager() );
        
        final TapePartition partition1 = mockDaoDriver.createTapePartition( null, null );
        final TapePartition partition2 = mockDaoDriver.createTapePartition( null, null );

        final TapeDrive drive1 = mockDaoDriver.createTapeDrive( partition1.getId(), null );
        mockDaoDriver.updateBean( drive1.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        final TapeDrive drive2 = mockDaoDriver.createTapeDrive( partition2.getId(), null );
        mockDaoDriver.updateBean( drive2.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        final TapeDrive drive3 = mockDaoDriver.createTapeDrive( partition2.getId(), null );
        mockDaoDriver.updateBean( drive3.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        
        support.ensureAvailableTapeDrivesAreUpToDate();
        
        final BeansRetriever< TapePartitionFailure > failureRetriever =
                dbSupport.getServiceManager().getRetriever( TapePartitionFailure.class );
        assertEquals(
                0,
                failureRetriever.getCount(),
                "Should notta generated failure yet."
                 );
        
        mockDaoDriver.updateBean( drive2.setState( TapeDriveState.ERROR ), TapeDrive.STATE );
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                0,
                failureRetriever.getCount(),
                "Should notta generated failure yet."
                 );
        
        mockDaoDriver.updateBean( drive3.setState( TapeDriveState.OFFLINE ), TapeDrive.STATE );
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                1,
                failureRetriever.getCount(),
                "Shoulda generated failure."
                 );
        
        mockDaoDriver.updateBean( drive1.setState( TapeDriveState.ERROR ), TapeDrive.STATE );
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                2,
                failureRetriever.getCount(),
                "Shoulda generated failure."
                 );
        
        mockDaoDriver.updateBean( drive1.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                1,
                failureRetriever.getCount(),
                "Shoulda deleted failure."
                 );
        
        mockDaoDriver.updateBean( drive2.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                0,
                failureRetriever.getCount(),
                "Shoulda deleted failure."
                 );
        
        mockDaoDriver.updateBean( drive2.setState( TapeDriveState.ERROR ), TapeDrive.STATE );
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                1,
                failureRetriever.getCount(),
                "Shoulda generated failure."
                );
        
        mockDaoDriver.updateBean( drive3.setState( TapeDriveState.NORMAL ), TapeDrive.STATE );
        support.ensureAvailableTapeDrivesAreUpToDate();
        assertEquals(
                0,
                failureRetriever.getCount(),
                "Shoulda deleted failure."
                 );
    }
    
    
    private InvocationHandler getRpcClientIh()
    {
        return MockInvocationHandler.forReturnType( 
                RpcResource.class,
                new ConstantResponseInvocationHandler( new MockTapeDriveResource() ),
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
