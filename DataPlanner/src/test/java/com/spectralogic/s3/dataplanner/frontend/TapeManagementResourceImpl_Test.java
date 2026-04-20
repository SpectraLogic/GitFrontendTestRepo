/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.frontend;

import java.lang.reflect.Array;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.ImportPersistenceTargetDirectiveRequest;
import com.spectralogic.s3.common.rpc.dataplanner.domain.TapeFailuresInformation;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeBlobStore;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.ForceFlagRequiredException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.server.RpcServer;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.thread.CronRunnableExecutor;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class TapeManagementResourceImpl_Test 
{
    @Test
    public void testConstructorNullRpcServerNotAllowed()
    {
        
        TestUtil.assertThrows( null, NullPointerException.class, () -> new TapeManagementResourceImpl(
                null,
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ),
                dbSupport.getServiceManager() ) );
    }
    
    
    @Test
    public void testConstructorNullTapeBlobStoreNotAllowed()
    {
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> new TapeManagementResourceImpl(
                rpcServer,
                null,
                dbSupport.getServiceManager() ) );
    }
    

    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> new TapeManagementResourceImpl(
                rpcServer,
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ),
                null ) );
    }
    

    @Test
    public void testConstructorHappyConstruction()
    {
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        new TapeManagementResourceImpl( 
                rpcServer,
                InterfaceProxyFactory.getProxy( TapeBlobStore.class, null ),
                dbSupport.getServiceManager() );
    }
    
    
    @Test
    public void testInspectTapeNonNullTapeIdInspectsSingleTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.inspectTape( tape1.getId(), null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "inspectTape" ) );
        assertEquals(1,  invocations.size(), "Shoulda inspected single tape.");
        final Object expected = tape1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 1 ), "Shoulda inspected single tape.");
        assertEquals(BlobStoreTaskPriority.LOW, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda inspected at low priority.");
    }
    
    
    @Test
    public void testInspectTapeNullTapeIdInspectsAllTapes()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTape( TapeState.INCOMPATIBLE );
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.inspectTape( null, null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "inspectTape" ) );
        assertEquals(2,  invocations.size(), "Shoulda inspected all tapes.");
        assertEquals(BlobStoreTaskPriority.LOW, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda inspected at low priority.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 1 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 1 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda inspected all tapes.");
    }
    
    
    @Test
    public void testInspectTapeNullTapeIdWhenInspectThrowsExceptionStillInspectsAllTapes()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        for ( int i = 0; i < 10; ++i )
        {
            mockDaoDriver.createTape();
        }
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( ( proxy, method, args ) -> {
            throw new RuntimeException( "You can't inspect that tape since I say so." );
        } );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.inspectTape( null, null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "inspectTape" ) );
        assertEquals(10,  invocations.size(), "Shoulda inspected all tapes.");
    }
    
    
    @Test
    public void testInspectTapeWithValidPriorityAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.inspectTape( null, BlobStoreTaskPriority.HIGH );
    }
    
    
    @Test
    public void testVerifyTapeNonNullTapeIdVerifysSingleTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.verifyTape( tape1.getId(), null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( BlobStore.class, "verify" ) );
        assertEquals(1,  invocations.size(), "Shoulda verifyed single tape.");
        final Object expected = tape1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 1 ), "Shoulda verifyed single tape.");
        assertEquals(BlobStoreTaskPriority.LOW, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda verifyed at low priority.");
    }
    
    
    @Test
    public void testVerifyTapeNullTapeIdVerifysAllTapes()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.NORMAL );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.NORMAL );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tape1.getPartitionId(), tape1.getType() );
        mockDaoDriver.updateBean( 
                tape1.setStorageDomainMemberId( sdm.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean( 
                tape2.setStorageDomainMemberId( sdm.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.PENDING_INSPECTION );
        mockDaoDriver.createTape( TapeState.PENDING_INSPECTION );
        mockDaoDriver.createTape( TapeState.PENDING_INSPECTION );
        mockDaoDriver.createTape( TapeState.FOREIGN );
        mockDaoDriver.createTape( TapeState.FOREIGN );
        mockDaoDriver.createTape( TapeState.FOREIGN );
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.verifyTape( null, null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( BlobStore.class, "verify" ) );
        assertEquals(2,  invocations.size(), "Shoulda verifyed all tapes.");
        assertEquals(BlobStoreTaskPriority.LOW, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda verifyed at low priority.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 1 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 1 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda verifyed all tapes.");
    }
    
    
    @Test
    public void testVerifyTapeNullTapeIdWhenVerifyThrowsExceptionStillVerifysAllTapes()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        for ( int i = 0; i < 10; ++i )
        {
            mockDaoDriver.createTape( TapeState.NORMAL );
        }
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), null, TapeType.LTO5 );
        mockDaoDriver.updateAllBeans( 
                BeanFactory.newBean( Tape.class ).setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( ( proxy, method, args ) -> {
            throw new RuntimeException( "You can't verify that tape since I say so." );
        } );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.verifyTape( null, null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( BlobStore.class, "verify" ) );
        assertEquals(10,  invocations.size(), "Shoulda verifyed all tapes.");
    }
    
    
    @Test
    public void testVerifyTapeWithValidPriorityAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.verifyTape( null, BlobStoreTaskPriority.HIGH );
    }
    
    
    @Test
    public void testVerifyTapeWithInvalidPriorityNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST,
                () -> resource.verifyTape( null, BlobStoreTaskPriority.BACKGROUND ) );
    }
    
    
    @Test
    public void testCleanDriveNonNullTapeIdCleansSingleDrive()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        final TapeDrive drive = mockDaoDriver.createTapeDrive( null, "aa" );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cleanDrive( drive.getId() );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "cleanDrive" ) );
        assertEquals(1,  invocations.size(), "Shoulda inspected single tape.");
        final Object expected = drive.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda inspected single tape.");
    }
    
    
    @Test
    public void testFormatTapeNonNullTapeIdForcedFormatSingleTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );

        assertNull(resource.formatTape( tape1.getId(), true, false), "Should notta returned failure payload since formatting single tape.");
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "formatTape" ) );
        assertEquals(1,  invocations.size(), "Shoulda formatted single tape.");
        final Object expected = tape1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 1 ), "Shoulda formatted single tape.");
        assertEquals(BlobStoreTaskPriority.LOW, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda formatted at low priority.");
        assertEquals(Boolean.TRUE, invocations.get( 0 ).getArgs().get( 2 ), "Shoulda sent force flag correctly.");
    }
    
    
    @Test
    public void testFormatTapeNonNullTapeIdNotForcedFormatSingleTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );

        assertNull(resource.formatTape( tape1.getId(), false, false), "Should notta returned failure payload since formatting single tape.");
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "formatTape" ) );
        assertEquals(1,  invocations.size(), "Shoulda formatted single tape.");
        final Object expected = tape1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 1 ), "Shoulda formatted single tape.");
        assertEquals(BlobStoreTaskPriority.LOW, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda formatted at low priority.");
        assertEquals(Boolean.FALSE, invocations.get( 0 ).getArgs().get( 2 ), "Shoulda sent force flag correctly.");
    }
    
    
    @Test
    public void testFormatTapeNonNullTapeIdWhereFormatCallOntoBlobStoreThrowsForceFlagRequiredNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( ( proxy, method, args ) -> {
            throw new ForceFlagRequiredException( "I said so" );
        } );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        TestUtil.assertThrows( null, ForceFlagRequiredException.class,
                () -> resource.formatTape( tape1.getId(), false, false) );
    }
    
    
    @Test
    public void testFormatTapeNonNullTapeIdWhereFormatCallOntoBlobStoreThrowsNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( ( proxy, method, args ) -> {
            throw new IllegalStateException( "I said so" );
        } );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        TestUtil.assertThrows( null, IllegalStateException.class, () -> resource.formatTape( tape1.getId(), false, false) );
    }
    
    
    @Test
    public void testFormatTapeNullTapeIdForcedFormatsAllTapes()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        final TapeFailuresInformation retval = resource.formatTape( null, true, false).getWithoutBlocking();
        assertEquals(0,  retval.getFailures().length, "Should notta been any failures.");
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "formatTape" ) );
        assertEquals(2,  invocations.size(), "Shoulda formatted all tapes.");
        assertEquals(BlobStoreTaskPriority.LOW, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda formatted at low priority.");
        assertEquals(Boolean.TRUE, invocations.get( 0 ).getArgs().get( 2 ), "Shoulda sent force flag correctly.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 1 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 1 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda formatted all tapes.");
    }
    
    
    @Test
    public void testFormatTapeNullTapeIdNotForcedFormatsAllTapes()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTape( TapeState.LOST );
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );

        final TapeFailuresInformation retval = resource.formatTape( null, false, false).getWithoutBlocking();
        assertEquals(0,  retval.getFailures().length, "Should notta been any failures.");
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "formatTape" ) );
        assertEquals(2,  invocations.size(), "Shoulda formatted all tapes.");
        assertEquals(BlobStoreTaskPriority.LOW, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda formatted at low priority.");
        assertEquals(Boolean.FALSE, invocations.get( 0 ).getArgs().get( 2 ), "Shoulda sent force flag correctly.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 1 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 1 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda formatted all tapes.");
    }
    
    
    @Test
    public void testFormatTapeNullTapeIdFormatThrowsExceptionStillFormatsAllTapes()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        for ( int i = 0; i < 10; ++i )
        {
            mockDaoDriver.createTape();
        }
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( ( proxy, method, args ) -> {
            throw new RuntimeException( "You can't format that tape since I say so." );
        } );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        final TapeFailuresInformation retval = resource.formatTape( null, false, false).getWithoutBlocking();
        assertEquals(10,  retval.getFailures().length, "Shoulda been some failures.");
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "formatTape" ) );
        assertEquals(10,  invocations.size(), "Shoulda formatted all tapes.");
    }
    
    
    @Test
    public void testCancelFormatTapeNonNullTapeIdCancelsFormatForSpecifiedTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelFormatTape( tape1.getId() );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "cancelFormatTape" ) );
        assertEquals(1,  invocations.size(), "Shoulda canceled format for single tape.");
        final Object expected = tape1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda canceled format for single tape.");
    }
    
    
    @Test
    public void testCancelFormatTapeNullTapeIdCancelsFormatForEveryTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.FORMAT_PENDING );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.FORMAT_PENDING );
        mockDaoDriver.createTape( TapeState.FORMAT_IN_PROGRESS );
        mockDaoDriver.createTape( TapeState.NORMAL );
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelFormatTape( null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "cancelFormatTape" ) );
        assertEquals(2,  invocations.size(), "Shoulda canceled format for all tapes that were format pending.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 0 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 0 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda canceled format for all tapes.");
    }
    
    
    @Test
    public void testImportTapeNonNullTapeIdImportsSingleTape() 
            throws NoSuchMethodException, SecurityException
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        final UUID userId = UUID.randomUUID();
        assertNull(resource.importTape( 
                        tape1.getId(), 
                        newImportDirective( userId, dataPolicy.getId(), storageDomain.getId() ) ), "Should notta returned failure payload since importing single tape.");
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( TapeBlobStore.class.getMethod( 
                        "importTape", BlobStoreTaskPriority.class, ImportTapeDirective.class ) );
        assertEquals(1,  invocations.size(), "Shoulda imported single tape.");
        final Object expected2 = tape1.getId();
        assertEquals(expected2, ( (ImportTapeDirective)invocations.get( 0 ).getArgs().get( 1 ) ).getTapeId(), "Shoulda imported single tape.");
        assertEquals(BlobStoreTaskPriority.HIGH, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda imported at high priority.");
        assertEquals(userId, ( (ImportTapeDirective)invocations.get( 0 ).getArgs().get( 1 ) ).getUserId(), "Shoulda sent user id correctly.");
        final Object expected1 = dataPolicy.getId();
        assertEquals(expected1, ( (ImportTapeDirective)invocations.get( 0 ).getArgs().get( 1 ) ).getDataPolicyId(), "Shoulda sent user id correctly.");
        final Object expected = storageDomain.getId();
        assertEquals(expected, ( (ImportTapeDirective)invocations.get( 0 ).getArgs().get( 1 ) ).getStorageDomainId(), "Shoulda sent user id correctly.");
    }
    
    
    @Test
    public void testImportTapeNullTapeIdImportsAllTapes() throws NoSuchMethodException, SecurityException
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.FOREIGN );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.FOREIGN );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );

        final UUID userId = UUID.randomUUID();
        final TapeFailuresInformation retval = resource.importTape( 
                null,
                newImportDirective(
                        userId, dataPolicy.getId(), storageDomain.getId() ) ).getWithoutBlocking();
        assertEquals(0,  retval.getFailures().length, "Should notta been any failures.");
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( TapeBlobStore.class.getMethod( 
                        "importTape", BlobStoreTaskPriority.class, ImportTapeDirective.class ) );
        assertEquals(2,  invocations.size(), "Shoulda imported all tapes.");
        assertEquals(BlobStoreTaskPriority.HIGH, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda imported at high priority.");
        assertEquals(userId, ( (ImportTapeDirective)invocations.get( 0 ).getArgs().get( 1 ) ).getUserId(), "Shoulda sent user id correctly.");
        final Object expected1 = dataPolicy.getId();
        assertEquals(expected1, ( (ImportTapeDirective)invocations.get( 0 ).getArgs().get( 1 ) ).getDataPolicyId(), "Shoulda sent user id correctly.");
        final Object expected = storageDomain.getId();
        assertEquals(expected, ( (ImportTapeDirective)invocations.get( 0 ).getArgs().get( 1 ) ).getStorageDomainId(), "Shoulda sent user id correctly.");
        tapeIds.remove( ( (ImportTapeDirective)invocations.get( 0 ).getArgs().get( 1 ) ).getTapeId() );
        tapeIds.remove( ( (ImportTapeDirective)invocations.get( 1 ).getArgs().get( 1 ) ).getTapeId() );
        assertEquals(0,  tapeIds.size(), "Shoulda imported all tapes.");
    }
    
    
    @Test
    public void testImportTapeNullTapeIdThrowsExceptionStillImportsAllTapes() 
            throws NoSuchMethodException, SecurityException
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        for ( int i = 0; i < 10; ++i )
        {
            mockDaoDriver.createTape( TapeState.FOREIGN );
        }
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( ( proxy, method, args ) -> {
            throw new RuntimeException( "You can't import that tape since I say so." );
        } );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        final TapeFailuresInformation retval = resource.importTape( 
                null,
                newImportDirective(
                        UUID.randomUUID(), 
                        dataPolicy.getId(),
                        null ) ).getWithoutBlocking();
        assertEquals(10,  retval.getFailures().length, "Shoulda been some failures.");
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( TapeBlobStore.class.getMethod( 
                        "importTape", BlobStoreTaskPriority.class, ImportTapeDirective.class ) );
        assertEquals(10,  invocations.size(), "Shoulda imported all tapes.");
    }
    
    
    @Test
    public void testRawImportTapeNonNullTapeIdImportsSingleTape()
            throws NoSuchMethodException, SecurityException
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createABMConfigSingleCopyOnTape();
        final Bucket bucket = mockDaoDriver.createBucket( null,  dp.getId(), "b1" );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        final UUID tapeId = tape1.getId();
        assertNull(resource.rawImportTape( 
                        tapeId,
                        bucket.getId(),
                        BeanFactory.newBean( ImportPersistenceTargetDirectiveRequest.class )
                    	.setPriority( BlobStoreTaskPriority.HIGH ) ), "Should notta returned failure payload since importing single tape.");
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( TapeBlobStore.class.getMethod( 
                        "importTape", BlobStoreTaskPriority.class, RawImportTapeDirective.class ) );
        assertEquals(1,  invocations.size(), "Shoulda imported single tape.");
        final Object expected1 = tape1.getId();
        assertEquals(expected1, ( (RawImportTapeDirective)invocations.get( 0 ).getArgs().get( 1 ) ).getTapeId(), "Shoulda imported single tape.");
        assertEquals(BlobStoreTaskPriority.HIGH, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda imported at high priority.");
        final Object expected = bucket.getId();
        assertEquals(expected, ( (RawImportTapeDirective)invocations.get( 0 ).getArgs().get( 1 ) ).getBucketId(), "Shoulda sent bucket id correctly.");
    }
    
    
    @Test
    public void testRawImportTapeNullTapeIdImportsAllTapes() throws NoSuchMethodException, SecurityException
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.LTFS_WITH_FOREIGN_DATA );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.LTFS_WITH_FOREIGN_DATA );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );

        final UUID bucketId = UUID.randomUUID();
        final TapeFailuresInformation retval = resource.rawImportTape( 
                null,
                bucketId,
                BeanFactory.newBean( ImportPersistenceTargetDirectiveRequest.class )
                	.setPriority( BlobStoreTaskPriority.HIGH ) ).getWithoutBlocking();
        assertEquals(0,  retval.getFailures().length, "Should notta been any failures.");
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( TapeBlobStore.class.getMethod( 
                        "importTape", BlobStoreTaskPriority.class, RawImportTapeDirective.class ) );
        assertEquals(2,  invocations.size(), "Shoulda imported all tapes.");
        assertEquals(BlobStoreTaskPriority.HIGH, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda imported at high priority.");
        assertEquals(bucketId, ( (RawImportTapeDirective)invocations.get( 0 ).getArgs().get( 1 ) ).getBucketId(), "Shoulda sent user id correctly.");
        tapeIds.remove( ( (RawImportTapeDirective)invocations.get( 0 ).getArgs().get( 1 ) ).getTapeId() );
        tapeIds.remove( ( (RawImportTapeDirective)invocations.get( 1 ).getArgs().get( 1 ) ).getTapeId() );
        assertEquals(0,  tapeIds.size(), "Shoulda imported all tapes.");
    }
    
    
    @Test
    public void testRawImportTapeNullTapeIdThrowsExceptionStillImportsAllTapes()
            throws NoSuchMethodException, SecurityException
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        for ( int i = 0; i < 10; ++i )
        {
            mockDaoDriver.createTape( TapeState.LTFS_WITH_FOREIGN_DATA );
        }
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( ( proxy, method, args ) -> {
            throw new RuntimeException( "You can't import that tape since I say so." );
        } );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );

        final TapeFailuresInformation retval = resource.rawImportTape( 
                null,
                UUID.randomUUID(),
                BeanFactory.newBean( ImportPersistenceTargetDirectiveRequest.class )
            	.setPriority( BlobStoreTaskPriority.values()[ 0 ] ) ).getWithoutBlocking();
        assertEquals(10,  retval.getFailures().length, "Shoulda been some failures.");
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( TapeBlobStore.class.getMethod( 
                        "importTape", BlobStoreTaskPriority.class, RawImportTapeDirective.class ) );
        assertEquals(10,  invocations.size(), "Shoulda imported all tapes.");
    }
    
    
    @Test
    public void testCancelRawImportTapeNonNullTapeIdCancelsImportForSpecifiedTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape( TapeState.LTFS_WITH_FOREIGN_DATA );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelImportTape( tape1.getId() );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "cancelImportTape" ) );
        assertEquals(1,  invocations.size(), "Shoulda canceled import for single tape.");
        final Object expected = tape1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda canceled import for single tape.");
    }
    
    
    @Test
    public void testCancelRawImportTapeNullTapeIdCancelsImportForEveryTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.RAW_IMPORT_PENDING );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.RAW_IMPORT_PENDING );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelImportTape( null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "cancelImportTape" ) );
        assertEquals(2,  invocations.size(), "Shoulda canceled format for all tapes.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 0 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 0 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda canceled format for all tapes.");
    }
    
    
    @Test
    public void testCancelImportTapeNonNullTapeIdCancelsImportForSpecifiedTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape( TapeState.FOREIGN );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelImportTape( tape1.getId() );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "cancelImportTape" ) );
        assertEquals(1,  invocations.size(), "Shoulda canceled import for single tape.");
        final Object expected = tape1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda canceled import for single tape.");
    }
    
    
    @Test
    public void testCancelImportTapeNullTapeIdCancelsImportForEveryTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.IMPORT_PENDING );
        final Tape tape2 = mockDaoDriver.createTape( TapeState.IMPORT_PENDING );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelImportTape( null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "cancelImportTape" ) );
        assertEquals(2,  invocations.size(), "Shoulda canceled format for all tapes.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 0 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 0 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda canceled format for all tapes.");
    }
    
    
    @Test
    public void testEjectTapeNonNullTapeIdEjectsSingleTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.ejectTape( tape1.getId(), null, null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "ejectTape" ) );
        assertEquals(1,  invocations.size(), "Shoulda ejected single tape.");
        final Object expected = tape1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 1 ), "Shoulda ejected single tape.");
    }
    
    
    @Test
    public void testEjectTapeNullTapeIdEjectsAllTapes()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.LOST );
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.ejectTape( null, null, null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "ejectTape" ) );
        assertEquals(2,  invocations.size(), "Shoulda ejected all tapes.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 1 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 1 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda ejected all tapes.");
    }
    
    
    @Test
    public void testEjectTapeNullTapeIdWhenEjectThrowsExceptionStillEjectsAllTapes()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        for ( int i = 0; i < 10; ++i )
        {
            mockDaoDriver.createTape();
        }
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.LOST );
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( ( proxy, method, args ) -> {
            throw new RuntimeException( "You can't eject that tape since I say so." );
        } );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        final TapeFailuresInformation failures = resource.ejectTape( null, null, null ).getWithoutBlocking();
        assertEquals(10,  failures.getFailures().length, "Shoulda thrown exception noting which tapes couldn't be ejected.");
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "ejectTape" ) );
        assertEquals(10,  invocations.size(), "Shoulda ejected all tapes.");
    }
    
    
    @Test
    public void testOnlineTapeNonNullTapeIdOnlinesSingleTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.onlineTape( tape1.getId() );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "onlineTape" ) );
        assertEquals(1,  invocations.size(), "Shoulda onlined single tape.");
        final Object expected = tape1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda onlined single tape.");
    }
    
    
    @Test
    public void testOnlineTapeNullTapeIdOnlinesAllTapes()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape( TapeState.OFFLINE );
        final Tape tape2 = mockDaoDriver.createTape(TapeState.OFFLINE );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTape( TapeState.ONLINE_IN_PROGRESS );
        mockDaoDriver.createTape( TapeState.ONLINE_IN_PROGRESS );
        mockDaoDriver.createTape( TapeState.ONLINE_IN_PROGRESS );
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.onlineTape( null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "onlineTape" ) );
        assertEquals(2,  invocations.size(), "Shoulda onlined all tapes.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 0 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 0 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda onlined all tapes.");
    }
    
    
    @Test
    public void testCancelOnlineTapeNonNullTapeIdEjectsSingleTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelOnlineTape( tape1.getId() );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "cancelOnlineTape" ) );
        assertEquals(1,  invocations.size(), "Shoulda canceled online for single tape.");
        final Object expected = tape1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda canceled online for single tape.");
    }
    
    
    @Test
    public void testCancelOnlineTapeNullTapeIdEjectsAllTapes()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.ONLINE_IN_PROGRESS );
        mockDaoDriver.createTape( TapeState.OFFLINE );
        mockDaoDriver.createTape( TapeState.LOST );
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );
        
        dbSupport.getServiceManager().getService( TapeService.class )
                .transistState( tape1, TapeState.ONLINE_PENDING );
        dbSupport.getServiceManager().getService( TapeService.class )
            .transistState( tape2, TapeState.ONLINE_PENDING );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelOnlineTape( null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "cancelOnlineTape" ) );
        assertEquals(2,  invocations.size(), "Shoulda canceled online for all tapes.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 0 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 0 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda canceled online for all tapes.");
    }
    
    
    @Test
    public void testCancelEjectTapeNonNullTapeIdEjectsSingleTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forReturnType( 
                        boolean.class, 
                        new ConstantResponseInvocationHandler( Boolean.TRUE ), 
                        null ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelEjectTape( tape1.getId() );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "cancelEjectTape" ) );
        assertEquals(1,  invocations.size(), "Shoulda canceled eject for single tape.");
        final Object expected = tape1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda canceled eject for single tape.");
    }
    
    
    @Test
    public void testCancelEjectTapeFailsIfCannotEjectTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );

        resource.cancelEjectTape( tape1.getId() );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "cancelEjectTape" ) );
        assertEquals(1,  invocations.size(), "Shoulda canceled eject for single tape.");
        final Object expected = tape1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda canceled eject for single tape.");
    }
    
    
    @Test
    public void testCancelEjectTapeNullTapeIdEjectsAllTapes()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        mockDaoDriver.createTape( TapeState.EJECT_FROM_EE_PENDING );
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTape( TapeState.LOST );
        mockDaoDriver.createTape( TapeState.LOST );
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );
        
        dbSupport.getServiceManager().getService( TapeService.class ).update( 
                tape1.setEjectPending( new Date() ), Tape.EJECT_PENDING );
        dbSupport.getServiceManager().getService( TapeService.class ).update( 
                tape2.setEjectPending( new Date() ), Tape.EJECT_PENDING );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelEjectTape( null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "cancelEjectTape" ) );
        assertEquals(3,  invocations.size(), "Shoulda canceled eject for all tapes.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 0 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 0 ) );
        tapeIds.remove( invocations.get( 2 ).getArgs().get( 0 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda canceled eject for all tapes.");
    }
    
    
    @Test
    public void testCancelVerifyTapeNonNullTapeIdCancelsVerifyOnSingleTape()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler(
                MockInvocationHandler.forReturnType( 
                        boolean.class, 
                        new ConstantResponseInvocationHandler( Boolean.TRUE ), 
                        null ) );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelVerifyTape( tape1.getId() );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "cancelVerifyTape" ) );
        assertEquals(1,  invocations.size(), "Shoulda canceled verify for single tape.");
        final Object expected = tape1.getId();
        assertEquals(expected, invocations.get( 0 ).getArgs().get( 0 ), "Shoulda canceled verify for single tape.");
    }
    
    
    @Test
    public void testCancelVerifyTapeNullTapeIdCancelsAllTapes()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        final Tape tape3 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );
        
        dbSupport.getServiceManager().getService( TapeService.class ).update( 
                tape1.setVerifyPending( BlobStoreTaskPriority.values()[ 1 ] ), Tape.VERIFY_PENDING );
        dbSupport.getServiceManager().getService( TapeService.class ).update( 
                tape2.setVerifyPending( BlobStoreTaskPriority.values()[ 1 ] ), Tape.VERIFY_PENDING );
        dbSupport.getServiceManager().getService( TapeService.class ).update( 
                tape3.setVerifyPending( BlobStoreTaskPriority.values()[ 1 ] ), Tape.VERIFY_PENDING );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.cancelVerifyTape( null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "cancelVerifyTape" ) );
        assertEquals(3,  invocations.size(), "Shoulda canceled verify for all tapes.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 0 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 0 ) );
        tapeIds.remove( invocations.get( 2 ).getArgs().get( 0 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda canceled verify for all tapes.");
    }
    
    
    @Test
    public void testEjectStorageDomainBlobIdsContainsNullNotAllowed()
    {
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> resource.ejectStorageDomain( UUID.randomUUID(), null, null, null, new UUID [] { null } ) );
    }
    
    
    @Test
    public void testEjectStorageDomainNullBucketIdEjectsAllTapesInStorageDomain()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        final Tape tape3 = mockDaoDriver.createTape();
        final Tape tape4 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.LOST );
        
        final Bucket bucket = mockDaoDriver.createBucket( null, "a" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), null, TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape1.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape2.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape3.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                mockDaoDriver.createStorageDomain( "osd" ).getId(),
                tape4.getPartitionId(),
                tape4.getType() );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape4.setStorageDomainMemberId( sdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        mockDaoDriver.updateBean( tape1.setBucketId( bucket.getId() ), PersistenceTarget.BUCKET_ID );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape3.getId(), b2.getId() );
        
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId(), tape3.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.ejectStorageDomain( storageDomain.getId(), null, null, null, null );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "ejectTape" ) );
        assertEquals(3,  invocations.size(), "Shoulda ejected all tapes in storage domain.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 1 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 1 ) );
        tapeIds.remove( invocations.get( 2 ).getArgs().get( 1 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda ejected all tapes in storage domain.");
    }
    
    
    @Test
    public void testEjectStorageDomainNonNullBucketIdEjectsAllTapesInStorageDomainUsedByBucket()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        final Tape tape3 = mockDaoDriver.createTape();
        final Tape tape4 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.LOST );
        
        final Bucket bucket = mockDaoDriver.createBucket( null, "a" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), null, TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape1.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape2.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape3.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                mockDaoDriver.createStorageDomain( "osd" ).getId(),
                tape4.getPartitionId(),
                tape4.getType() );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape4.setStorageDomainMemberId( sdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        mockDaoDriver.updateBean( tape1.setBucketId( bucket.getId() ), PersistenceTarget.BUCKET_ID );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape3.getId(), b2.getId() );
        
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.ejectStorageDomain( 
                storageDomain.getId(),
                bucket.getId(),
                null,
                null, 
                (UUID[])Array.newInstance( UUID.class, 0 ) );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "ejectTape" ) );
        assertEquals(2,  invocations.size(), "Shoulda ejected all tapes in storage domain used by bucket.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 1 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 1 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda ejected all tapes in storage domain used by bucket.");
    }
    
    
    @Test
    public void testEjectStorageDomainNonNullBucketIdAndBlobsEjectsAllTapesInStorageDomainUsedByBucket()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        final Tape tape3 = mockDaoDriver.createTape();
        final Tape tape4 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.LOST );
        
        final Bucket bucket = mockDaoDriver.createBucket( null, "a" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), null, TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape1.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape2.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape3.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                mockDaoDriver.createStorageDomain( "osd" ).getId(),
                tape4.getPartitionId(),
                tape4.getType() );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape4.setStorageDomainMemberId( sdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        mockDaoDriver.updateBean( tape1.setBucketId( bucket.getId() ), PersistenceTarget.BUCKET_ID );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape3.getId(), b2.getId() );
        
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape2.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.ejectStorageDomain(
                storageDomain.getId(), bucket.getId(), null, null, new UUID [] { b1.getId() } );
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "ejectTape" ) );
        assertEquals(1,  invocations.size(), "Shoulda ejected all tapes in storage domain used by bucket.");
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 1 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda ejected all tapes in storage domain used by bucket.");
    }

    
    @Test
    public void testForceTapeEnvironmentRefreshDelegatesToTapeBlobStore()
    {
        

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        resource.forceTapeEnvironmentRefresh();
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( 
                        ReflectUtil.getMethod( BlobStore.class, "refreshEnvironmentNow" ) );
        assertEquals(1,  invocations.size(), "Shoulda forced refresh of tape environment.");
    }
    
    
    @Test
    public void testQuiesceAndPrepareForShutdownWhenNotForcedDoesShutDownReturns()
    {
        

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final Tape tape = mockDaoDriver.createTape( TapeState.FOREIGN );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final TapeBlobStore dataStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                dataStore,
                dbSupport.getServiceManager() );
        
        final Duration duration = new Duration();
        resource.quiesceAndPrepareForShutdown( false );
        assertEquals(0,  duration.getElapsedSeconds(), "Shoulda shut down immediately.");

        resource.inspectTape( tape.getId(), BlobStoreTaskPriority.HIGH );
        TestUtil.assertThrows( null, GenericFailure.RETRY_WITH_ASYNCHRONOUS_WAIT, () -> resource.importTape(
                tape.getId(),
                newImportDirective(
                        bucket.getUserId(),
                        mockDaoDriver.getDataPolicyFor( bucket.getId() ).getId(),
                        null ) ) );
    }
    
    
    @Test
    public void testQuiesceAndPrepareForShutdownWhenNotForcedDoesNotShutDownThrows()
    {
        

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape( TapeState.IMPORT_IN_PROGRESS );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final TapeBlobStore dataStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                dataStore,
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.FORCE_FLAG_REQUIRED,
                () -> resource.quiesceAndPrepareForShutdown( false ) );
    }
    
    
    @Test
    public void testQuiesceAndPrepareForShutdownWhenForcedDoesShutDownReturns()
    {
        

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.IMPORT_PENDING );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( 
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final TapeBlobStore dataStore = InterfaceProxyFactory.getProxy(
                TapeBlobStore.class, ( proxy, method, args ) -> {
                    if ( !method.getName().equals( "cancelImportTape" ) )
                    {
                        throw new UnsupportedOperationException( "Method not supported: " + method );
                    }
                    dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                            tape, TapeState.FOREIGN );
                    return Boolean.TRUE;
                } );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                dataStore,
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.FORCE_FLAG_REQUIRED,
                () -> resource.quiesceAndPrepareForShutdown( false ) );
        TestUtil.assertThrows( null, GenericFailure.FORCE_FLAG_REQUIRED,
                () -> resource.quiesceAndPrepareForShutdown( false ) );
        
        resource.quiesceAndPrepareForShutdown( true );

        assertEquals(0,  dbSupport.getServiceManager().getRetriever(ImportTapeDirective.class).getCount(), "Shoulda whacked any import tape directives.");
    }
    
    
    @Test
    public void testQuiesceAndPrepareForShutdownWhenForcedDoesNotShutDownThrows()
    {
        

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createTape( TapeState.IMPORT_PENDING );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final TapeBlobStore dataStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, null );
        final TapeManagementResource resource = new TapeManagementResourceImpl( 
                rpcServer,
                dataStore,
                dbSupport.getServiceManager() );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, () -> resource.quiesceAndPrepareForShutdown( true ) );
    }
    
    
    @Test
    public void testCronBasedAutoEjectTriggersConfiguredCorrectlyInitiallyAndUpdatedCorrectlyUponRefresh()
    {
        

        new MockCacheFilesystemDriver( dbSupport ).shutdown();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain sd0 = mockDaoDriver.createStorageDomain( "sd0" );
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain sd3 = mockDaoDriver.createStorageDomain( "sd3" );
        mockDaoDriver.updateBean( 
                sd0.setAutoEjectUponCron( "0 15 10 L * ?" ),
                StorageDomain.AUTO_EJECT_UPON_CRON );
        mockDaoDriver.updateBean( 
                sd2.setAutoEjectUponCron( "0 15 10 L * ?" ),
                StorageDomain.AUTO_EJECT_UPON_CRON );
        mockDaoDriver.updateBean( 
                sd3.setAutoEjectUponCron( "0 15 9 L * ?" ),
                StorageDomain.AUTO_EJECT_UPON_CRON );
        
        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final TapeBlobStore dataStore = InterfaceProxyFactory.getProxy(
                TapeBlobStore.class,
                MockInvocationHandler.forReturnType(
                        Object.class, new ConstantResponseInvocationHandler( new Object() ), null ) );
        final TapeManagementResourceImpl resource = new TapeManagementResourceImpl( 
                rpcServer,
                dataStore,
                dbSupport.getServiceManager() );
        assertTrue(CronRunnableExecutor.isScheduled( resource.getCronIdentifier( sd0.getId() ) ), "Should notta scheduled a CRON job except for sd0, sd2 and sd3.");
        assertFalse(
                CronRunnableExecutor.isScheduled( resource.getCronIdentifier( sd1.getId() ) ),
                "Should notta scheduled a CRON job except for sd0, sd2 and sd3."
                 );
        assertTrue(CronRunnableExecutor.isScheduled( resource.getCronIdentifier( sd2.getId() ) ), "Should notta scheduled a CRON job except for sd0, sd2 and sd3.");
        assertTrue(CronRunnableExecutor.isScheduled( resource.getCronIdentifier( sd3.getId() ) ), "Should notta scheduled a CRON job except for sd0, sd2 and sd3.");

        final StorageDomain sd4 = mockDaoDriver.createStorageDomain( "sd4" );
        final StorageDomain sd5 = mockDaoDriver.createStorageDomain( "sd5" );
        mockDaoDriver.updateBean( 
                sd0.setAutoEjectUponCron( "0 15 1 L * ?" ),
                StorageDomain.AUTO_EJECT_UPON_CRON );
        mockDaoDriver.updateBean( 
                sd1.setAutoEjectUponCron( "0 15 1 L * ?" ),
                StorageDomain.AUTO_EJECT_UPON_CRON );
        mockDaoDriver.updateBean( 
                sd2.setAutoEjectUponCron( null ),
                StorageDomain.AUTO_EJECT_UPON_CRON );
        mockDaoDriver.updateBean( 
                sd4.setAutoEjectUponCron( "0 15 3 L * ?" ),
                StorageDomain.AUTO_EJECT_UPON_CRON );
        
        resource.refreshStorageDomainAutoEjectCronTriggers();
        assertTrue(CronRunnableExecutor.isScheduled( resource.getCronIdentifier( sd0.getId() ) ), "Should notta scheduled a CRON job except for sd0, sd1, sd3, and sd4.");
        assertTrue(CronRunnableExecutor.isScheduled( resource.getCronIdentifier( sd1.getId() ) ), "Should notta scheduled a CRON job except for sd0, sd1, sd3, and sd4.");
        assertFalse(
                CronRunnableExecutor.isScheduled( resource.getCronIdentifier( sd2.getId() ) ),
                "Should notta scheduled a CRON job except for sd0, sd1, sd3, and sd4."
                 );
        assertTrue(CronRunnableExecutor.isScheduled( resource.getCronIdentifier( sd3.getId() ) ), "Should notta scheduled a CRON job except for sd0, sd1, sd3, and sd4.");
        assertTrue(CronRunnableExecutor.isScheduled( resource.getCronIdentifier( sd4.getId() ) ), "Should notta scheduled a CRON job except for sd0, sd1, sd3, and sd4.");
        assertFalse(
                CronRunnableExecutor.isScheduled( resource.getCronIdentifier( sd5.getId() ) ),
                "Should notta scheduled a CRON job except for sd0, sd1, sd3, and sd4."
                 );
    }
    
    
    @Test
    public void testAutoEjectTriggerEjectsTapesAppropriately()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        final Tape tape3 = mockDaoDriver.createTape();
        final Tape tape4 = mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape();
        mockDaoDriver.createTape( TapeState.EJECTED );
        mockDaoDriver.createTape( TapeState.LOST );
        
        final Bucket bucket = mockDaoDriver.createBucket( null, "a" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "b" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final StorageDomain storageDomain = 
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        mockDaoDriver.updateBean(
                storageDomain.setAutoEjectUponCron( "0/1 * * * * ?" ), StorageDomain.AUTO_EJECT_UPON_CRON );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), null, TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape1.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape2.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape3.setStorageDomainMemberId( sdm.getId() ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                mockDaoDriver.createStorageDomain( "osd" ).getId(),
                tape4.getPartitionId(),
                tape4.getType() );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape4.setStorageDomainMemberId( sdm1.getId() ), 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        
        mockDaoDriver.updateBean( tape1.setBucketId( bucket.getId() ), PersistenceTarget.BUCKET_ID );
        final S3Object o1 = mockDaoDriver.createObject( bucket.getId(), "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2" );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        mockDaoDriver.putBlobOnTape( tape2.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape3.getId(), b2.getId() );
        
        final Set< UUID > tapeIds = CollectionFactory.toSet( tape1.getId(), tape2.getId(), tape3.getId() );

        final RpcServer rpcServer = InterfaceProxyFactory.getProxy( RpcServer.class, null );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final TapeBlobStore tapeBlobStore = InterfaceProxyFactory.getProxy( TapeBlobStore.class, btih );
        final TapeManagementResourceImpl resource = new TapeManagementResourceImpl( 
                rpcServer,
                tapeBlobStore,
                dbSupport.getServiceManager() );
        
        int i = 1000;
        while ( --i > 0 && 3 > btih.getMethodCallCount( 
                ReflectUtil.getMethod( TapeBlobStore.class, "ejectTape" ) ) )
        {
            TestUtil.sleep( 10 );
        }
        final List< MethodInvokeData > invocations =
                btih.getMethodInvokeData( ReflectUtil.getMethod( TapeBlobStore.class, "ejectTape" ) );
        tapeIds.remove( invocations.get( 0 ).getArgs().get( 1 ) );
        tapeIds.remove( invocations.get( 1 ).getArgs().get( 1 ) );
        tapeIds.remove( invocations.get( 2 ).getArgs().get( 1 ) );
        assertEquals(0,  tapeIds.size(), "Shoulda ejected all tapes in storage domain.");

        CronRunnableExecutor.unschedule( resource.getCronIdentifier( storageDomain.getId() ) );
    }
    
    
    private ImportPersistenceTargetDirectiveRequest newImportDirective(
            final UUID userId,
            final UUID dataPolicyId,
            final UUID storageDomainId )
    {
        return BeanFactory.newBean( ImportPersistenceTargetDirectiveRequest.class )
                .setUserId( userId ).setDataPolicyId( dataPolicyId ).setStorageDomainId( storageDomainId )
                .setPriority( BlobStoreTaskPriority.HIGH );
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
