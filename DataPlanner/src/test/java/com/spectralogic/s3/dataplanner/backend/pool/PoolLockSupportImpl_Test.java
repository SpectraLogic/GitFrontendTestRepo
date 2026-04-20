/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.api.BlobPoolLastAccessedUpdater;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockingException;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolPowerManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolQuiescedManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.NullInvocationHandler;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class PoolLockSupportImpl_Test 
{
    @Test
    public void testConstructorNullBlobLastAccessedUpdaterNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {

        public void test() throws Throwable
            {
                new PoolLockSupportImpl<>( 
                        null, 
                        InterfaceProxyFactory.getProxy( PoolPowerManager.class, null ), 
                        InterfaceProxyFactory.getProxy( PoolQuiescedManager.class, null ) );
            }
        } );
    }
    
    
    @Test
    public void testConstructorNullPowerManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new PoolLockSupportImpl<>( 
                        InterfaceProxyFactory.getProxy( BlobPoolLastAccessedUpdater.class, null ), 
                        null, 
                        InterfaceProxyFactory.getProxy( PoolQuiescedManager.class, null ) );
            }
        } );
    }
    
    
    @Test
    public void testConstructorNullQuiescedManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                new PoolLockSupportImpl<>( 
                        InterfaceProxyFactory.getProxy( BlobPoolLastAccessedUpdater.class, null ), 
                        InterfaceProxyFactory.getProxy( PoolPowerManager.class, null ), 
                        null );
            }
        } );
    }
    
    
    @Test
    public void testAcquireReadBlobLockNullPoolIdNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = null;
        final UUID blobId = UUID.randomUUID();
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireReadLock( poolId, blobId );
            }
        } );
        callVerifier.assertNoPowerCalls();
        callVerifier.assertBlobAccessedCalled();
    }
    
    
    @Test
    public void testAcquireReadBlobLockNullBlobIdNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        final UUID blobId = null;
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireReadLock( poolId, blobId );
            }
        } );
        callVerifier.assertNoPowerCalls();
        callVerifier.assertBlobAccessedCalled();
    }
    
    
    @Test
    public void testAcquireReadBlobLockWhenWriteLockAcquiredAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        final UUID blobId = UUID.randomUUID();
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 2 ), 10, 100);
        callVerifier.assertPowerOnCalled( poolId );
        
        lockSupport.acquireReadLock( poolId, blobId );
        callVerifier.assertPowerCallsMade( 1, 0 );
        callVerifier.assertBlobAccessedCalled( blobId );
    }
    
    
    @Test
    public void testAcquireReadBlobLockWhenExclusiveLockAcquiredNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        final UUID blobId = UUID.randomUUID();
        lockSupport.acquireExclusiveLock( poolId, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId );
        
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireReadLock( poolId, blobId );
            }
        } );
        callVerifier.assertNoPowerCalls();
        callVerifier.assertBlobAccessedCalled( blobId );
    }
    
    
    @Test
    public void testAcquireReadBlobLockDoesSo()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId1 = UUID.randomUUID();
        final UUID poolId2 = UUID.randomUUID();
        final UUID blobId1 = UUID.randomUUID();
        final UUID blobId2 = UUID.randomUUID();
        lockSupport.acquireReadLock( poolId1, blobId1 );
        callVerifier.assertPowerOnCalled( poolId1 );
        callVerifier.assertBlobAccessedCalled( blobId1 );
        lockSupport.acquireReadLock( poolId2, blobId1 );
        callVerifier.assertPowerOnCalled( poolId2 );
        callVerifier.assertBlobAccessedCalled( blobId1 );
        lockSupport.acquireReadLock( poolId1, blobId1 );
        callVerifier.assertPowerOnCalled( poolId1 );
        callVerifier.assertBlobAccessedCalled( blobId1 );
        lockSupport.acquireReadLock( poolId2, blobId2 );
        callVerifier.assertPowerOnCalled( poolId2 );
        callVerifier.assertBlobAccessedCalled( blobId2 );
        lockSupport.acquireReadLock( poolId2, blobId2 );
        callVerifier.assertPowerOnCalled( poolId2 );
        callVerifier.assertBlobAccessedCalled( blobId2 );

        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireExclusiveLock( poolId1, Integer.valueOf( 2 ) );
            }
        } );
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireExclusiveLock( poolId2, Integer.valueOf( 2 ) );
            }
        } );
        callVerifier.assertNoPowerCalls();
        callVerifier.assertBlobAccessedCalled();
        final Object expected = CollectionFactory.toSet( blobId1, blobId2 );
        assertEquals(expected, lockSupport.getBlobLockHolders(), "Shoulda reported blob lock holders correctly.");
    }
    
    
    @Test
    public void testAcquireReadBlobLockWhenPowerManagerThrowsExceptionDoesNotTakeLock()
    {
        final PoolLockSupport< Integer > lockSupport = newLockSupportWithThrowingPowerManager();
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireReadLock( UUID.randomUUID(), UUID.randomUUID() );
            }
        } );
        assertTrue(lockSupport.getPoolsUnavailableForExclusiveLock().isEmpty(), "Should notta acquired any read lock due to power failure.");
    }
    
    
    @Test
    public void testAcquireReadLockNullPoolNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = null;
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireReadLock( poolId, Integer.valueOf( 2 ) );
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireReadLockNullLockHolderNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireReadLock( poolId, (Integer)null );
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireReadLockWhenAlreadyWriteLockedAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 2 ), 10, 100);
        callVerifier.assertPowerOnCalled( poolId );
        lockSupport.acquireReadLock( poolId, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId );
    }
    
    
    @Test
    public void testAcquireReadLockWhenAlreadyExclusivelyLockedNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        lockSupport.acquireExclusiveLock( poolId, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId );
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireReadLock( poolId, Integer.valueOf( 2 ) );
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireReadLockWhenAlreadyReadLockedAllowedProvidedThatNonReentrantContractNotViolated()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        lockSupport.acquireReadLock( poolId, UUID.randomUUID() );
        callVerifier.assertPowerOnCalled( poolId );
        lockSupport.acquireReadLock( poolId, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId );
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireReadLock( poolId, Integer.valueOf( 2 ) );
            }
        } );
        
        lockSupport.acquireReadLock( poolId, Integer.valueOf( 3 ) );
        callVerifier.assertPowerOnCalled( poolId );
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireReadLock( poolId, Integer.valueOf( 2 ) );
            }
        } );

        assertTrue(lockSupport.releaseLock( Integer.valueOf( 3 ) ), "Shoulda been a lock to release.");
        callVerifier.assertNoPowerCalls();
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireReadLock( poolId, Integer.valueOf( 2 ) );
            }
        } );

        assertTrue(lockSupport.releaseLock( Integer.valueOf( 2 ) ), "Shoulda been a lock to release.");
        callVerifier.assertNoPowerCalls();
        lockSupport.acquireReadLock( poolId, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId );
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireReadLock( poolId, Integer.valueOf( 2 ) );
            }
        } );
        callVerifier.assertNoPowerCalls();

        assertTrue(lockSupport.releaseLock( Integer.valueOf( 2 ) ), "Shoulda been a lock to release.");
        callVerifier.assertNoPowerCalls();
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 2 ), 10, 100);
        callVerifier.assertPowerOnCalled( poolId );
    }
    
    
    @Test
    public void testAcquireReadLockWhenPowerManagerThrowsExceptionDoesNotTakeLock()
    {
        final PoolLockSupport< Integer > lockSupport = newLockSupportWithThrowingPowerManager();
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireReadLock( UUID.randomUUID(), Integer.valueOf( 2 ) );
            }
        } );
        assertTrue(lockSupport.getPoolsUnavailableForExclusiveLock().isEmpty(), "Should notta acquired any read lock due to power failure.");
    }
    
    
    @Test
    public void testAcquireWriteLockNullPoolNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = null;
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireWriteLock( poolId, Integer.valueOf( 2 ), 10, 100);
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireDeleteLockNullPoolNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = null;
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> lockSupport.acquireDeleteLock( poolId, 2 ) );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireWriteLockNullLockHolderNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireWriteLock( poolId, null, 10, 100 );
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireDeleteLockNullLockHolderNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> lockSupport.acquireDeleteLock( poolId, null ) );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireWriteLockWhenAlreadyWriteLockedAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 2 ), 10, 100);
        callVerifier.assertPowerOnCalled( poolId );
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 3 ), 10, 100 );
        callVerifier.assertPowerOnCalled( poolId );
    }


    @Test
    public void testAcquireWriteLockNotEnoughSpaceNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();

        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 2 ), 40, 100);
        callVerifier.assertPowerOnCalled( poolId );
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 3 ), 40, 100 );
        callVerifier.assertPowerOnCalled( poolId );
        TestUtil.assertThrows( null, PoolLockingException.class, () -> lockSupport.acquireWriteLock( poolId, Integer.valueOf( 4 ), 40, 100 ) );
        callVerifier.assertNoPowerCalls();
        lockSupport.releaseLock(Integer.valueOf(2));
        //We are still using an in-memory value for "space available" so we will ignore the 100 passed in
        TestUtil.assertThrows( null, PoolLockingException.class, () -> lockSupport.acquireWriteLock( poolId, Integer.valueOf( 4 ), 40, 100 ) );
        callVerifier.assertNoPowerCalls();
        lockSupport.releaseLock(Integer.valueOf(3));
        //We've released the last lock for this pool, so now we will head the 100 passed in
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 4 ), 40, 100 );
        //callVerifier.assertPowerOnCalled( poolId );
        callVerifier.assertPowerCallsMade(1, 1);
    }

    @Test
    public void testPoolsWithWriteLockNotUnavailableForWriteLock() {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 2 ), 40, 100);
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 3 ), 40, 100 );
        assertTrue(lockSupport.getPoolsUnavailableForWriteLock().isEmpty());
    }

    @Test
    public void testPoolsWithInsufficientSpaceUnavailableForWriteLock() {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 2 ), 40, 100);
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 3 ), 40, 100 );
        assertTrue( lockSupport.getPoolsUnavailableForWriteLock().isEmpty());
        assertTrue(lockSupport.getPoolsUnavailableForWriteLock(20).isEmpty());
        assertFalse(lockSupport.getPoolsUnavailableForWriteLock(21).isEmpty());
    }
    
    @Test
    public void testAcquireDeleteLockWhenAlreadyWriteLockedNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        lockSupport.acquireWriteLock( poolId, 2, 10, 100 );
        callVerifier.assertPowerOnCalled( poolId );
        TestUtil.assertThrows( null, PoolLockingException.class, () -> lockSupport.acquireDeleteLock( poolId, 3 ) );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireDeleteLockWaitDoesWait()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        
        lockSupport.acquireWriteLock( poolId, 2, 10, 100 );
        callVerifier.assertPowerOnCalled( poolId );
        
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future< ? > future = executor.submit( () -> lockSupport.acquireDeleteLockWait( poolId, 3 ) );
        lockSupport.releaseLock( 2 );
    
        try
        {
            future.get( 100, TimeUnit.MILLISECONDS );
        }
        catch ( final InterruptedException | ExecutionException | TimeoutException e )
        {
            throw new RuntimeException( "Failed trying to get a delete lock while waiting" );
        }
    }
    
    
    @Test
    public void testAcquireWriteLockWhenAlreadyExclusivelyLockedNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        lockSupport.acquireExclusiveLock( poolId, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId );
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireWriteLock( poolId, Integer.valueOf( 2 ), 10, 100);
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireDeleteLockWhenAlreadyExclusivelyLockedNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        lockSupport.acquireExclusiveLock( poolId, 2 );
        callVerifier.assertPowerOnCalled( poolId );
        TestUtil.assertThrows( null, PoolLockingException.class, () -> lockSupport.acquireDeleteLock( poolId, 3 ) );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireWriteLockWhenAlreadyReadBlobLockedAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        lockSupport.acquireReadLock( poolId, UUID.randomUUID() );
        callVerifier.assertPowerOnCalled( poolId );
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 2 ), 10, 100);
        callVerifier.assertPowerOnCalled( poolId );
    }
    
    
    @Test
    public void testAcquireDeleteLockWhenAlreadyReadBlobLockedAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        lockSupport.acquireReadLock( poolId, UUID.randomUUID() );
        callVerifier.assertPowerOnCalled( poolId );
        lockSupport.acquireDeleteLock( poolId, 2 );
        callVerifier.assertPowerOnCalled( poolId );
    }
    
    
    @Test
    public void testAcquireWriteLockWhenAlreadyReadLockedAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        lockSupport.acquireReadLock( poolId, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId );
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 2 ), 10, 100);
        callVerifier.assertPowerOnCalled( poolId );
    }
    
    
    @Test
    public void testAcquireWriteLockWhenPowerManagerThrowsExceptionDoesNotTakeLock()
    {
        final PoolLockSupport< Integer > lockSupport = newLockSupportWithThrowingPowerManager();
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireWriteLock( UUID.randomUUID(), Integer.valueOf( 2 ), 10, 100 );
            }
        } );
        assertTrue(lockSupport.getPoolsUnavailableForExclusiveLock().isEmpty(), "Should notta acquired any read lock due to power failure.");
    }
    
    
    @Test
    public void testAcquireExclusiveLockNullPoolNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = null;
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireExclusiveLock( poolId, Integer.valueOf( 2 ) );
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireExclusiveLockNullLockHolderNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireExclusiveLock( poolId, null );
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireExclusiveLockWhenAlreadyWriteLockedNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 2 ), 10, 100);
        callVerifier.assertPowerOnCalled( poolId );
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireExclusiveLock( poolId, Integer.valueOf( 2 ) );
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireExclusiveLockWhenAlreadyExclusivelyLockedNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        lockSupport.acquireExclusiveLock( poolId, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId );
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireExclusiveLock( poolId, Integer.valueOf( 2 ) );
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireExclusiveLockWhenAlreadyReadBlobLockedNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        lockSupport.acquireReadLock( poolId, UUID.randomUUID() );
        callVerifier.assertPowerOnCalled( poolId );
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireExclusiveLock( poolId, Integer.valueOf( 2 ) );
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireExclusiveLockWhenAlreadyReadLockedNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        
        lockSupport.acquireReadLock( poolId, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId );
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireExclusiveLock( poolId, Integer.valueOf( 2 ) );
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testAcquireExclusiveLockWhenPowerManagerThrowsExceptionDoesNotTakeLock()
    {
        final PoolLockSupport< Integer > lockSupport = newLockSupportWithThrowingPowerManager();
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireExclusiveLock( UUID.randomUUID(), Integer.valueOf( 2 ) );
            }
        } );
        assertTrue(lockSupport.getPoolsUnavailableForExclusiveLock().isEmpty(), "Should notta acquired any read lock due to power failure.");
    }
    
    
    @Test
    public void testReleaseBlobReadLocksNullBlobIdsNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.releaseBlobLocks( null );
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testReleaseBlobReadLocksWorks()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        
        final UUID pool1 = UUID.randomUUID();
        final UUID pool2 = UUID.randomUUID();
        
        final UUID blob1 = UUID.randomUUID();
        final UUID blob2 = UUID.randomUUID();
        
        lockSupport.acquireReadLock( pool1, blob1 );
        callVerifier.assertPowerOnCalled( pool1 );
        final Object expected5 = CollectionFactory.toSet( blob1 );
        assertEquals(expected5, lockSupport.getBlobLockHolders(), "Shoulda reported blob read locks correctly.");

        lockSupport.acquireReadLock( pool1, blob2 );
        callVerifier.assertPowerOnCalled( pool1 );
        final Object expected4 = CollectionFactory.toSet( blob1, blob2 );
        assertEquals(expected4, lockSupport.getBlobLockHolders(), "Shoulda reported blob read locks correctly.");

        lockSupport.acquireReadLock( pool2, blob2 );
        callVerifier.assertPowerOnCalled( pool2 );
        final Object expected3 = CollectionFactory.toSet( blob1, blob2 );
        assertEquals(expected3, lockSupport.getBlobLockHolders(), "Shoulda reported blob read locks correctly.");

        lockSupport.releaseBlobLocks( CollectionFactory.toSet( blob1, blob2 ) );
        callVerifier.assertPowerCallsMade( 0, 2 );
        final Object expected2 = CollectionFactory.toSet();
        assertEquals(expected2, lockSupport.getBlobLockHolders(), "Shoulda reported blob read locks correctly.");

        lockSupport.acquireReadLock( pool1, blob1 );
        lockSupport.acquireReadLock( pool1, blob2 );
        lockSupport.acquireReadLock( pool2, blob2 );
        callVerifier.assertPowerOnCalled( pool1, pool1, pool2 );
        
        lockSupport.releaseBlobLocks( CollectionFactory.toSet( blob1 ) );
        callVerifier.assertNoPowerCalls();
        final Object expected1 = CollectionFactory.toSet( blob2 );
        assertEquals(expected1, lockSupport.getBlobLockHolders(), "Shoulda reported blob read locks correctly.");

        lockSupport.releaseBlobLocks( CollectionFactory.toSet( blob2, UUID.randomUUID() ) );
        callVerifier.assertPowerCallsMade( 0, 2 );
        final Object expected = CollectionFactory.toSet();
        assertEquals(expected, lockSupport.getBlobLockHolders(), "Shoulda reported blob read locks correctly.");
    }
    
    
    @Test
    public void testReleaseLockNullLockHolderNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.releaseLock( null );
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testReleaseLockForLockHolderNotLockingAnythingNotAllowed()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );

        assertFalse(lockSupport.releaseLock( Integer.valueOf( 2 ) ), "Should notta been a lock to release.");
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testReleaseLockForLockHolderHoldingReadLockDoesSo()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        lockSupport.acquireReadLock( poolId, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId );
        assertTrue(lockSupport.releaseLock( Integer.valueOf( 2 ) ), "Shoulda been a lock to release.");
        callVerifier.assertPowerOffCalled( poolId );
        lockSupport.acquireReadLock( poolId, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId );
        
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireReadLock( poolId, Integer.valueOf( 2 ) );
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testReleaseLockForLockHolderHoldingWriteLockDoesSo()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 2 ), 10, 100);
        callVerifier.assertPowerOnCalled( poolId );
        assertTrue(lockSupport.releaseLock( Integer.valueOf( 2 ) ), "Shoulda been a lock to release.");
        callVerifier.assertPowerOffCalled( poolId );
        lockSupport.acquireWriteLock( poolId, Integer.valueOf( 2 ), 10, 100);
        callVerifier.assertPowerOnCalled( poolId );
    }
    
    
    @Test
    public void testReleaseLockForLockHolderHoldingExclusiveLockDoesSo()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId = UUID.randomUUID();
        lockSupport.acquireExclusiveLock( poolId, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId );
        assertTrue(lockSupport.releaseLock( Integer.valueOf( 2 ) ), "Shoulda been a lock to release.");
        callVerifier.assertPowerOffCalled( poolId );
        lockSupport.acquireExclusiveLock( poolId, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId );
    }
    
    
    @Test
    public void testReleaseLockForLockHolderHoldingWriteAndExclusiveLocksDoesSo()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId1 = UUID.randomUUID();
        final UUID poolId2 = UUID.randomUUID();
        final UUID poolId3 = UUID.randomUUID();
        lockSupport.acquireWriteLock( poolId1, Integer.valueOf( 2 ), 10, 100 );
        callVerifier.assertPowerOnCalled( poolId1 );
        lockSupport.acquireExclusiveLock( poolId2, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId2 );
        lockSupport.acquireExclusiveLock( poolId3, Integer.valueOf( 3 ) );
        callVerifier.assertPowerOnCalled( poolId3 );
        assertTrue(lockSupport.releaseLock( Integer.valueOf( 2 ) ), "Shoulda been a lock to release.");
        callVerifier.assertPowerOffCalled( poolId1, poolId2 );
        lockSupport.acquireWriteLock( poolId1, Integer.valueOf( 2 ), 10, 100 );
        callVerifier.assertPowerOnCalled( poolId1 );
        lockSupport.acquireExclusiveLock( poolId2, Integer.valueOf( 2 ) );
        callVerifier.assertPowerOnCalled( poolId2 );
        
        TestUtil.assertThrows( null, PoolLockingException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.acquireExclusiveLock( poolId3, Integer.valueOf( 2 ) );
            }
        } );
        callVerifier.assertNoPowerCalls();
    }
    
    
    @Test
    public void testReleaseLockWhenPowerManagerThrowsExceptionDoesReleaseLock()
    {
        final AtomicBoolean fail = new AtomicBoolean( false );
        final PoolLockSupport< Integer > lockSupport = newLockSupportWithThrowingPowerManager( fail );
        lockSupport.acquireReadLock( UUID.randomUUID(), Integer.valueOf( 2 ) );
        assertFalse(lockSupport.getPoolsUnavailableForExclusiveLock().isEmpty(), "Shoulda acquired lock.");

        fail.set( true );
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                lockSupport.releaseLock( Integer.valueOf( 2 ) );
            }
        } );
        assertTrue(lockSupport.getPoolsUnavailableForExclusiveLock().isEmpty(), "Shoulda released lock even though power change failed.");
    }
    
    
    @Test
    public void testGetPoolsUnavailableForWriteLockReturnsUnavailablePoolsCorrectly()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId1 = UUID.randomUUID();
        final UUID poolId2 = UUID.randomUUID();
        final UUID poolId3 = UUID.randomUUID();
        final UUID poolId4 = UUID.randomUUID();

        final Object actual1 = lockSupport.getPoolsUnavailableForWriteLock();
        assertEquals(new HashSet<UUID>(), actual1, "Shoulda computed unavailable pools correctly.");

        lockSupport.acquireReadLock( poolId1, Integer.valueOf( 2 ) );
        final Object actual = lockSupport.getPoolsUnavailableForWriteLock();
        assertEquals(new HashSet<UUID>(), actual, "Shoulda computed unavailable pools correctly.");

        lockSupport.acquireWriteLock( poolId2, Integer.valueOf( 3 ), 10, 100 );
        final Object expected2 = CollectionFactory.toSet();
        assertEquals(expected2, lockSupport.getPoolsUnavailableForWriteLock(), "Shoulda computed unavailable pools correctly.");

        lockSupport.acquireExclusiveLock( poolId3, Integer.valueOf( 4 ) );
        final Object expected1 = CollectionFactory.toSet( poolId3 );
        assertEquals(expected1, lockSupport.getPoolsUnavailableForWriteLock(), "Shoulda computed unavailable pools correctly.");

        lockSupport.acquireReadLock( poolId4, UUID.randomUUID() );
        final Object expected = CollectionFactory.toSet( poolId3 );
        assertEquals(expected, lockSupport.getPoolsUnavailableForWriteLock(), "Shoulda computed unavailable pools correctly.");
    }
    
    
    @Test
    public void testGetPoolsUnavailableForExclusiveLockReturnsUnavailablePoolsCorrectly()
    {
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        final UUID poolId1 = UUID.randomUUID();
        final UUID poolId2 = UUID.randomUUID();
        final UUID poolId3 = UUID.randomUUID();
        final UUID poolId4 = UUID.randomUUID();

        final Object actual = lockSupport.getPoolsUnavailableForExclusiveLock();
        assertEquals(new HashSet<UUID>(), actual, "Shoulda computed unavailable pools correctly.");

        lockSupport.acquireReadLock( poolId1, Integer.valueOf( 2 ) );
        final Object expected3 = CollectionFactory.toSet( poolId1 );
        assertEquals(expected3, lockSupport.getPoolsUnavailableForExclusiveLock(), "Shoulda computed unavailable pools correctly.");

        lockSupport.acquireWriteLock( poolId2, Integer.valueOf( 3 ), 10, 100 );
        final Object expected2 = CollectionFactory.toSet( poolId1, poolId2 );
        assertEquals(expected2, lockSupport.getPoolsUnavailableForExclusiveLock(), "Shoulda computed unavailable pools correctly.");

        lockSupport.acquireExclusiveLock( poolId3, Integer.valueOf( 4 ) );
        final Object expected1 = CollectionFactory.toSet( poolId1, poolId2, poolId3 );
        assertEquals(expected1, lockSupport.getPoolsUnavailableForExclusiveLock(), "Shoulda computed unavailable pools correctly.");

        lockSupport.acquireReadLock( poolId4, UUID.randomUUID() );
        final Object expected = CollectionFactory.toSet( poolId1, poolId2, poolId3, poolId4 );
        assertEquals(expected, lockSupport.getPoolsUnavailableForExclusiveLock(), "Shoulda computed unavailable pools correctly.");
    }
    
    
    @Test
    public void testFullyQuiesceUnlockedPoolsThatAreQuiescePendingDoesSo()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool1 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( pool1.setQuiesced( Quiesced.NO ), Pool.QUIESCED );
        final Pool pool2 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( pool2.setQuiesced( Quiesced.PENDING ), Pool.QUIESCED );
        final Pool pool3 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( pool3.setQuiesced( Quiesced.PENDING ), Pool.QUIESCED );
        
        final CallVerifier callVerifier = new CallVerifier();
        final PoolLockSupport< Integer > lockSupport = newLockSupport( callVerifier );
        lockSupport.acquireReadLock( pool2.getId(), Integer.valueOf( 2 ) );
        lockSupport.fullyQuiesceUnlockedPoolsThatAreQuiescePending( dbSupport.getServiceManager() );
        
        mockDaoDriver.attainAndUpdate( pool1 );
        mockDaoDriver.attainAndUpdate( pool2 );
        mockDaoDriver.attainAndUpdate( pool3 );
        assertEquals(Quiesced.NO, pool1.getQuiesced(), "Shoulda left unquiesced pool alone.");
        assertEquals(Quiesced.PENDING, pool2.getQuiesced(), "Shoulda left quiesce-pending pool alone since it's locked.");
        assertEquals(Quiesced.YES, pool3.getQuiesced(), "Shoulda updated quiesce-pending pool since it's not locked so it's now quiesced.");
    }
    
    
    private final class CallVerifier
    {
        private CallVerifier()
        {
            m_expectedCalls.put( m_methodPowerOn, Integer.valueOf( 0 ) );
            m_expectedCalls.put( m_methodPowerOff, Integer.valueOf( 0 ) );
        }
        
        
        private void assertPowerOnCalled( final UUID ... poolIds )
        {
            for ( final UUID poolId : poolIds )
            {
                final int callCount = m_expectedCalls.get( m_methodPowerOn ).intValue();
                m_expectedCalls.put( m_methodPowerOn, Integer.valueOf( callCount + 1 ) );
                assertEquals(poolId, m_btih.getMethodInvokeData( m_methodPowerOn ).get( callCount ).getArgs().get( 0 ), "Shoulda passed in pool id to power on.");
            }
            assertNoPowerCalls();
        }
        
        
        private void assertPowerOffCalled( final UUID ... poolIds )
        {
            for ( final UUID poolId : poolIds )
            {
                final int callCount = m_expectedCalls.get( m_methodPowerOff ).intValue();
                m_expectedCalls.put( m_methodPowerOff, Integer.valueOf( callCount + 1 ) );
                assertEquals(poolId, m_btih.getMethodInvokeData( m_methodPowerOff ).get( callCount ).getArgs().get( 0 ), "Shoulda passed in pool id to power off.");
            }
            assertNoPowerCalls();
        }
        
        
        private void assertPowerCallsMade( final int newOnCalls, final int newOffCalls )
        {
            final int onCallCount = m_expectedCalls.get( m_methodPowerOn ).intValue();
            m_expectedCalls.put( m_methodPowerOn, Integer.valueOf( onCallCount + newOnCalls ) );
            
            final int offCallCount = m_expectedCalls.get( m_methodPowerOff ).intValue();
            m_expectedCalls.put( m_methodPowerOff, Integer.valueOf( offCallCount + newOffCalls ) );
            
            assertNoPowerCalls();
        }
        
        
        private void assertNoPowerCalls()
        {
            m_btih.verifyMethodInvocations( m_expectedCalls );
        }
        
        
        private void assertBlobAccessedCalled( final UUID ... blobIds )
        {
            final Set< UUID > expected = CollectionFactory.toSet( blobIds );
            m_blobAccessedCallCount += expected.size();
            final Map< Method, Integer > expectedCalls = new HashMap<>();
            expectedCalls.put( m_methodAccessed, Integer.valueOf( m_blobAccessedCallCount ) );
            m_blobLastAccessedUpdaterBtih.verifyMethodInvocations( expectedCalls );
            
            int offset = m_blobAccessedCallCount - expected.size();
            final Set< UUID > actual = new HashSet<>();
            for ( final MethodInvokeData mid 
                    : m_blobLastAccessedUpdaterBtih.getMethodInvokeData( m_methodAccessed ) )
            {
                --offset;
                if ( 0 <= offset )
                {
                    continue;
                }
                actual.add( (UUID)mid.getArgs().get( 0 ) );
            }

            assertEquals(expected, actual, "Shoulda called accessed for correct blobs.");
        }
        
        
        private int m_blobAccessedCallCount;
        private final Method m_methodPowerOn = ReflectUtil.getMethod( PoolPowerManager.class, "powerOn" );
        private final Method m_methodPowerOff = ReflectUtil.getMethod( PoolPowerManager.class, "powerOff" );
        private final Method m_methodAccessed = ReflectUtil.getMethod( 
                BlobPoolLastAccessedUpdater.class, "accessed" );
        private final Map< Method, Integer > m_expectedCalls = new HashMap<>();
        private final BasicTestsInvocationHandler m_btih = new BasicTestsInvocationHandler( null );
        private final BasicTestsInvocationHandler m_blobLastAccessedUpdaterBtih =
                new BasicTestsInvocationHandler( null );
    } // end inner class def
    
    
    private PoolLockSupport< Integer > newLockSupport( final CallVerifier callVerifier )
    {
        return new PoolLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy(
                        BlobPoolLastAccessedUpdater.class, callVerifier.m_blobLastAccessedUpdaterBtih ),
                InterfaceProxyFactory.getProxy( 
                        PoolPowerManager.class, callVerifier.m_btih ),
                InterfaceProxyFactory.getProxy( PoolQuiescedManager.class, null ) );
    }
    
    
    private PoolLockSupport< Integer > newLockSupportWithThrowingPowerManager()
    {
        return newLockSupportWithThrowingPowerManager( new AtomicBoolean( true ) );
    }
    
    
    private PoolLockSupport< Integer > newLockSupportWithThrowingPowerManager( final AtomicBoolean fail )
    {
        return new PoolLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy(
                        BlobPoolLastAccessedUpdater.class, null ),
                InterfaceProxyFactory.getProxy( 
                        PoolPowerManager.class, new InvocationHandler()
                        {
                            public Object invoke(
                                    final Object proxy,
                                    final Method method,
                                    final Object[] args ) throws Throwable
                            {
                                if ( fail.get() )
                                {
                                    throw new RuntimeException( "I can't do power stuff." );
                                }
                                return NullInvocationHandler.getInstance().invoke( proxy, method, args );
                            }
                        } ),
                InterfaceProxyFactory.getProxy( PoolQuiescedManager.class, null ) );
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public  void resetDB() {
        dbSupport.reset();
    }
}
