/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread.wp;


import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ThreadResettingThreadPoolExecutor_Test 
{

    @Test
    public void testThreadNameIsResetImmediatelyAfterHostedRunnableExits()
    {
        final String tn = "foo";
        
        final ThreadFactory tf = new ThreadFactory(){
            
            @Override public Thread newThread( final Runnable r )
            {
                return new ResettableThread( r, tn, false );
            }
        };
        
        final List<Thread> l = new LinkedList<>();
        
        final ThreadResettingThreadPoolExecutor trtpe =
               new ThreadResettingThreadPoolExecutor( 1, 1, 10,
               TimeUnit.SECONDS, new ArrayBlockingQueue< Runnable >( 10 ), tf );
        try
        {
            trtpe.execute( new Runnable()
                {
                    @Override public void run()
                    {
                        Thread.currentThread().setName( "bar" );
                        l.add( Thread.currentThread() );
                    }
                });
           
            int i = 0;
            while ( i++ < 40 && l.isEmpty() )
            {
                try
                {
                    Thread.sleep( 50 );
                }
                catch ( final InterruptedException ex )
                {
                     throw new RuntimeException( ex );
                }
            }
           
            final Thread t = l.get( 0 );
            i = 0;
            while ( i++ < 40 && ! tn.equals( t.getName() ) )
            {
                try
                {
                    Thread.sleep( 50 );
                }
                catch ( final InterruptedException ex )
                {
                    throw new RuntimeException( ex );
                }
            }
           
            assertEquals(
                    tn, l.get( 0 ).getName(),
                    "Strings should have been equal."
            );
        }
        finally
        {
            trtpe.shutdownNow();
        }
    }
}
