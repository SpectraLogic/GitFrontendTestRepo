/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.ds3target;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.interfaces.AbstractRequest;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.target.TargetLogger;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.mock.InterfaceProxyFactory;

final class ReplicationSourceIdentifierHeaderAddingDs3ClientFactory
{
    private ReplicationSourceIdentifierHeaderAddingDs3ClientFactory()
    {
        // singleton
    }
    
    
    static Ds3Client build( final UUID instanceId, final Ds3Client wrappedClient )
    {
        Validations.verifyNotNull( "Instance id", instanceId );
        Validations.verifyNotNull( "Wrapped client", wrappedClient );
        return InterfaceProxyFactory.getProxy( 
                Ds3Client.class, 
                new Ds3ClientInvocationHandler( instanceId, wrappedClient ) );
    }
    
    
    public static long getLastRequestNumber()
    {
        final Long retval = LAST_REQUEST_NUMBER.get( Long.valueOf( Thread.currentThread().getId() ) );
        if ( null == retval )
        {
            TargetLogger.LOG.warn( 
                    "No requests have been made on this thread, so can't determine last request number." );
            return -1;
        }
        
        return retval.longValue();
    }
    
    
    private final static class Ds3ClientInvocationHandler implements InvocationHandler
    {
        private Ds3ClientInvocationHandler( final UUID instanceId, final Ds3Client wrappedClient )
        {
            m_instanceId = instanceId;
            m_wrappedClient = wrappedClient;
        }
        
        
        public Object invoke( final Object proxy, final Method method, final Object[] args ) throws Throwable
        {
            if ( null != args && 1 == args.length && null != args[ 0 ]
                    && AbstractRequest.class.isAssignableFrom( args[ 0 ].getClass() ) )
            {
                final long requestNumber = REQUEST_NUMBER.getAndIncrement();
                synchronized ( LAST_REQUEST_NUMBER )
                {
                    LAST_REQUEST_NUMBER.put(
                            Long.valueOf( Thread.currentThread().getId() ), 
                            Long.valueOf( requestNumber ) );
                }
                
                final AbstractRequest request = (AbstractRequest)args[ 0 ];
                final String replicationSourceIdentifier =
                        m_instanceId.toString() + "<DS3-" + requestNumber + ">";
                request.getHeaders().put( 
                        S3HeaderType.REPLICATION_SOURCE_IDENTIFIER.getHttpHeaderName(),
                        replicationSourceIdentifier );
                TargetLogger.LOG.info( 
                        "Will send " + request.getClass().getSimpleName() + ": " 
                        + replicationSourceIdentifier );
            }
            
            return method.invoke( m_wrappedClient, args );
        }
        
        
        private final UUID m_instanceId;
        private final Ds3Client m_wrappedClient;
    } // end inner class def
    
    
    private final static Map< Long, Long > LAST_REQUEST_NUMBER = new HashMap<>();
    private final static AtomicLong REQUEST_NUMBER = new AtomicLong( 1 );
}
