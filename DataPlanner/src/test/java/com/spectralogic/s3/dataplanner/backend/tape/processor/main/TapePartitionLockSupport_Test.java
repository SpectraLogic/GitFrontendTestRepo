/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;



import com.spectralogic.s3.common.rpc.tape.TapePartitionResource;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class TapePartitionLockSupport_Test
{
    @Test
    public void testLockNullResourceNotAllowed()
    {
        final TapePartitionLockSupport support = new TapePartitionLockSupport();
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                support.lock( null );
            }
        } );
    }
    
    @Test
    public void testUnlockNullResourceNotAllowed()
    {
        final TapePartitionLockSupport support = new TapePartitionLockSupport();
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                support.unlock( null );
            }
        } );
    }
    
    @Test
    public void testLockResourceDoesSo()
    {
        final TapePartitionResource resource = 
                InterfaceProxyFactory.getProxy( TapePartitionResource.class, null );
        final TapePartitionLockSupport support = new TapePartitionLockSupport();

        final List< CountDownLatch > unlockLatches = new CopyOnWriteArrayList<>();
        final Runnable r1 = new Runnable()
        {
            public void run()
            {
                final TapePartitionResource resource2 = 
                        InterfaceProxyFactory.getProxy( TapePartitionResource.class, null );
                support.lock( resource2 );

                assertFalse(support.isLockedByCurrentThread( resource ), "Should notta reported resource as locked yet by this thread.");
                support.lock( resource );
                assertTrue(support.isLockedByCurrentThread( resource ), "Shoulda reported resource as locked by this thread.");
                support.lock( resource );
                assertTrue(support.isLockedByCurrentThread( resource ), "Shoulda reported resource as locked by this thread.");
                final CountDownLatch unlockLatch = new CountDownLatch( 1 );
                unlockLatches.add( unlockLatch );
                try
                {
                    unlockLatch.await();
                }
                catch ( final InterruptedException ex )
                {
                    throw new RuntimeException( ex );
                }

                support.unlock( resource );
                support.unlock( resource2 );
                assertTrue(support.isLockedByCurrentThread( resource ), "Shoulda reported resource as locked by this thread.");
                support.unlock( resource );
                assertFalse(support.isLockedByCurrentThread( resource ), "Should notta reported resource as locked by this thread.");
            }
        };
        final Runnable r2 = new Runnable()
        {
            public void run()
            {
                while ( !support.tryLock( resource ) )
                {
                    TestUtil.sleep( 1 );
                }
                final CountDownLatch unlockLatch = new CountDownLatch( 1 );
                unlockLatches.add( unlockLatch );
                try
                {
                    unlockLatch.await();
                }
                catch ( final InterruptedException ex )
                {
                    throw new RuntimeException( ex );
                }
                
                support.unlock( resource );
            }
        };
        for ( int i = 0; i < 5; ++i )
        {
            SystemWorkPool.getInstance().submit( r1 );
            SystemWorkPool.getInstance().submit( r2 );
        }
        
        for ( int i = 0; i < 10; ++i )
        {
            int j = 100;
            while ( --j > 0 && unlockLatches.isEmpty() )
            {
                TestUtil.sleep( 10 );
            }
            assertEquals(1,  unlockLatches.size(), "Shoulda had single consumer acquiring lock.");
            unlockLatches.remove( 0 ).countDown();
        }
    }
}
