/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.mock;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.rpc.dataplanner.domain.TapeFailureInformation;
import com.spectralogic.s3.common.rpc.dataplanner.domain.TapeFailuresInformation;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.server.RpcResponse;

public final class MediaOperationInvocationHandler implements InvocationHandler
{
    @Override
    public Object invoke( final Object proxy, final Method method, final Object[] args )
            throws Throwable
    {
        m_tapeIds.add( (UUID)args[ 0 ] );
        if ( 1 < args.length )
        {
            if ( boolean.class == method.getParameterTypes()[ 1 ] )
            {
                m_forceFlags.add( (Boolean)args[ 1 ] );
            }
            else if ( BlobStoreTaskPriority.class == method.getParameterTypes()[ 1 ] )
            {
                m_priorities.add( (BlobStoreTaskPriority)args[ 1 ] );
            }
        }
        
        if ( null == args[ 0 ] || !m_tapeFailures.isEmpty() )
        {
            return new RpcResponse<>( BeanFactory.newBean( TapeFailuresInformation.class )
                 .setFailures( CollectionFactory.toArray( TapeFailureInformation.class, m_tapeFailures ) ) );
        }
        return null;
    }
    
    
    public void addTapeFailure( final UUID tapeId, final String failure )
    {
        m_tapeFailures.add( BeanFactory.newBean( TapeFailureInformation.class )
                .setFailure( failure )
                .setTapeId( tapeId ) );
    }


    public List< UUID > getTapeIds()
    {
        return m_tapeIds;
    }


    public List< Boolean > getForceFlags()
    {
        return m_forceFlags;
    }
    
    
    public List< BlobStoreTaskPriority > getPriorities()
    {
        return m_priorities;
    }
    
    
    public void clear()
    {
        m_tapeIds.clear();
        m_forceFlags.clear();
        m_priorities.clear();
    }


    private final List< TapeFailureInformation > m_tapeFailures = new CopyOnWriteArrayList<>();
    private final List< UUID > m_tapeIds = new ArrayList<>();
    private final List< Boolean > m_forceFlags = new ArrayList<>();
    private final List< BlobStoreTaskPriority > m_priorities = new ArrayList<>();
}
