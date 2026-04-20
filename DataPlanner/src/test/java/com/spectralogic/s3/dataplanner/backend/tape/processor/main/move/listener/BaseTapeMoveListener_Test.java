/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.processor.main.move.listener;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;



import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class BaseTapeMoveListener_Test
{

    @Test
    public void testValidationCompletedSuccessResultsInSuccessCalledOnSubclass()
    {
        final ConcreteTapeMoveListener listener = new ConcreteTapeMoveListener();
        final ValidationWaiter waiter = new ValidationWaiter( listener );
        SystemWorkPool.getInstance().submit( waiter );
        TestUtil.sleep( 50 );
        assertNull(waiter.m_succeeded, "Should notta been done waiting yet.");
        listener.validationCompleted( UUID.randomUUID(), null );
        TestUtil.sleep( 50 );
        assertTrue(waiter.m_succeeded.booleanValue(), "Shoulda been done waiting since validation completed.");
        listener.waitUntilValidated();

        assertEquals(1,  listener.m_succeededCallCount.get(), "Shoulda made single validation completed call.");
        assertEquals(0,  listener.m_failedCallCount.get(), "Shoulda made single validation completed call.");
    }
    
    @Test
    public void testValidationCompletedFailedResultsInFailedCalledOnSubclass()
    {
        final ConcreteTapeMoveListener listener = new ConcreteTapeMoveListener();
        final ValidationWaiter waiter = new ValidationWaiter( listener );
        SystemWorkPool.getInstance().submit( waiter );
        TestUtil.sleep( 50 );
        assertNull(waiter.m_succeeded, "Should notta been done waiting yet.");
        listener.validationCompleted( UUID.randomUUID(), new IllegalStateException( "hiya" ) );
        TestUtil.sleep( 50 );
        assertTrue(waiter.m_succeeded.booleanValue(), "Shoulda been done waiting since validation completed.");
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                listener.waitUntilValidated();
            }
        } );

        assertEquals(0,  listener.m_succeededCallCount.get(), "Shoulda made single validation completed call.");
        assertEquals(1,  listener.m_failedCallCount.get(), "Shoulda made single validation completed call.");
    }
    
    
    private final static class ValidationWaiter implements Runnable
    {
        private ValidationWaiter( final BaseTapeMoveListener listener )
        {
            m_listener = listener;
        }
        
        public void run()
        {
            try
            {
                m_listener.waitUntilValidated();
            }
            catch ( final IllegalStateException ex )
            {
                Validations.verifyNotNull( "Shut up CodePro", ex );
                m_succeeded = Boolean.FALSE;
            }
            m_succeeded = Boolean.TRUE;
        }
        
        private volatile Boolean m_succeeded;
        private final BaseTapeMoveListener m_listener;
    } // end inner class def
    
    
    private final static class ConcreteTapeMoveListener extends BaseTapeMoveListener
    {
        public void moveSucceeded( final UUID tapeId )
        {
            throw new UnsupportedOperationException( "Not testing this." );
        }

        public void moveFailed( final UUID tapeId )
        {
            throw new UnsupportedOperationException( "Not testing this." );
        }

        @Override
        protected void validationSucceeded( final UUID tapeId )
        {
            Validations.verifyNotNull( "Tape id", tapeId );
            m_succeededCallCount.incrementAndGet();
        }

        @Override
        protected void validationFailed( final UUID tapeId, final RuntimeException failure )
        {
            Validations.verifyNotNull( "Tape id", tapeId );
            Validations.verifyNotNull( "Failure", failure );
            m_failedCallCount.incrementAndGet();
        }
        
        private final AtomicInteger m_succeededCallCount = new AtomicInteger();
        private final AtomicInteger m_failedCallCount = new AtomicInteger();
    } // end inner class def
}
