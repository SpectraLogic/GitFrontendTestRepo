/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.mock;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.UUID;

import com.spectralogic.util.net.rpc.server.RpcResponse;

public final class StartBlobWriteInvocationHandler implements InvocationHandler
{
    public StartBlobWriteInvocationHandler( final String destinationFile )
    {
        m_destinationFile = destinationFile;
    }
    
    
    public UUID getJobId()
    {
        return m_jobId;
    }
    
    
    public UUID getBlobId()
    {
        return m_blobId;
    }
    
    
    public int getCallCount()
    {
        return m_callCount;
    }
    
    
    public Object invoke( final Object proxy, final Method method, final Object[] args )
            throws Throwable
    {
        if ( args.length != 2
                || !UUID.class.isAssignableFrom( args[0].getClass() )
                || !UUID.class.isAssignableFrom( args[1].getClass() ) )
        {
            throw new IllegalArgumentException( "Shoulda provided two uuids." );
        }

        m_jobId = (UUID)args[0];
        m_blobId = (UUID)args[1];
        
        m_callCount++;
        
        return new RpcResponse<>( m_destinationFile );
    }

    
    private int m_callCount = 0;
    private final String m_destinationFile;
    private UUID m_blobId;
    private UUID m_jobId;
}
