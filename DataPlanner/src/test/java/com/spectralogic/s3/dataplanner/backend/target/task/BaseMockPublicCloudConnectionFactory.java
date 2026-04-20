/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnection;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnectionFactory;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.NullInvocationHandler;

abstract class BaseMockPublicCloudConnectionFactory
    < C extends PublicCloudConnection, T extends PublicCloudReplicationTarget< T > >
    implements PublicCloudConnectionFactory< C, T >
{
    protected BaseMockPublicCloudConnectionFactory( final Class< C > connectionType )
    {
        m_connectionType = connectionType;
    }
    
    
    synchronized public final C connect( final T target )
    {
        if ( null != m_connectException )
        {
            m_connectException.setStackTrace(new RuntimeException().getStackTrace());
            throw m_connectException;
        }
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( m_ih );
        m_btihs.add( btih );
        return InterfaceProxyFactory.getProxy( m_connectionType, btih );
    }
    
    
    synchronized public final void setIh( final InvocationHandler ih )
    {
        m_wrappedIh = ih;
    }
    
    
    synchronized public final void setConnectException( final RuntimeException ex )
    {
        m_connectException = ex;
    }
    
    
    synchronized public final BasicTestsInvocationHandler getSingleBtih()
    {
        if ( 1 != m_btihs.size() )
        {
            throw new IllegalStateException(
                    "Expected single connection, but " + m_btihs.size() + " connections were made." );
        }
        return m_btihs.get( 0 );
    }
    
    
    synchronized public final List< BasicTestsInvocationHandler > getBtihs()
    {
        return new ArrayList<>( m_btihs );
    }
    
    
    private final class ConnectionIh implements InvocationHandler
    {
        public Object invoke( final Object proxy, final Method method, final Object[] args ) throws Throwable
        {
            synchronized ( BaseMockPublicCloudConnectionFactory.this )
            {
                if ( null == m_wrappedIh )
                {
                    return NullInvocationHandler.getInstance().invoke( proxy, method, args );
                }
                return m_wrappedIh.invoke( proxy, method, args );
            }
        }
    } // end inner class def
    
    
    private InvocationHandler m_wrappedIh;
    private RuntimeException m_connectException =
            new RuntimeException( "I wasn't expecting any connect calls." );
    private final InvocationHandler m_ih = new ConnectionIh();
    private final List< BasicTestsInvocationHandler > m_btihs = new ArrayList<>();
    private final Class< C > m_connectionType;
}
