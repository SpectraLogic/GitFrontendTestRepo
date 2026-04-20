/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.thread.wp;

import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LoggingUncaughtExceptionHandler;
import com.spectralogic.util.net.rpc.domain.RpcFrameworkErrorCode;

final class RunnableAdapter implements Runnable
{
    RunnableAdapter( final Runnable task )
    {
        m_task = task;
        Validations.verifyNotNull( "Task", task );
    }
    
    
    public void run()
    {
        try
        {
            if ( ! Thread.currentThread().isInterrupted() )
            {
                m_task.run();
            }
        }
        catch (final FailureTypeObservableException ex) {
            LoggingUncaughtExceptionHandler.getInstance().uncaughtException( Thread.currentThread(), ex );
            throw ex;
        }
        catch ( final Exception ex )
        {
            LoggingUncaughtExceptionHandler.getInstance().uncaughtException( Thread.currentThread(), ex );
            throw ex;
        }
    }
    
    
    private final Runnable m_task;
}
