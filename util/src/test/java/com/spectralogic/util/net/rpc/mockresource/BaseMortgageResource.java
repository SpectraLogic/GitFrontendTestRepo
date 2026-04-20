/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.mockresource;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseRpcResource;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.testfrmwrk.TestUtil;


public abstract class BaseMortgageResource extends BaseRpcResource implements MortgageResourceMethods
{
    protected BaseMortgageResource()
    {
        Mortgage m = BeanFactory.newBean( Mortgage.class );
        m.setClientIds( new UUID [] { JOHN_ID, JASON_ID } );
        m.setMortgageId( MORTGAGE1_ID );
        m.setRemaining( 100 );
        m.setTotal( m.getRemaining() );
        m_mortgages.add( m );

        m = BeanFactory.newBean( Mortgage.class );
        m.setClientIds( new UUID [] { JASON_ID } );
        m.setMortgageId( MORTGAGE2_ID );
        m.setRemaining( 1000 );
        m.setTotal( m.getRemaining() );
        m_mortgages.add( m );

        m = BeanFactory.newBean( Mortgage.class );
        m.setClientIds( new UUID [] { BARRY_ID } );
        m.setMortgageId( MORTGAGE3_ID );
        m.setRemaining( 10000 );
        m.setTotal( m.getRemaining() );
        m_mortgages.add( m );
    }
    
    
    public void setSimulatedDelay( final int millis )
    {
        m_simulatedDelay = millis;
    }
    
    
    public void disableDynamicDelay()
    {
        m_dynamicDelay = false;
    }
    
    
    public void setCallCounter( final AtomicInteger callCounter )
    {
        m_callCounter = callCounter;
    }
    

    public RpcFuture< Integer > getSum( final int ... values )
    {
        simulateDelayToProcessRequest();
        
        if ( null == values )
        {
            throw new NullPointerException( "Null should never be the received input from frmwrk." );
        }
        
        int retval = 0;
        for ( final int v : values )
        {
            retval += v;
        }
        
        return new RpcResponse<>( Integer.valueOf( retval ) );
    }


    public RpcFuture< Integer > getMax( final Integer ... values )
    {
        simulateDelayToProcessRequest();
        
        if ( null == values )
        {
            throw new NullPointerException( "Null should never be the received input from frmwrk." );
        }
        
        int retval = values[ 0 ].intValue();
        for ( Integer v : values )
        {
            if ( null == v )
            {
                v = Integer.valueOf( 999 );
            }
            retval = Math.max( retval, v.intValue() );
        }
        
        return new RpcResponse<>( Integer.valueOf( retval ) );
    }
    

    public RpcFuture< Integer > performMath( final MathOperation operation, final Integer[] values )
    {
        simulateDelayToProcessRequest();
        
        switch ( operation )
        {
            case MAX:
                return getMax( values );
            case SUM:
                final int [] intValues = new int[ values.length ];
                for ( int i = 0; i < values.length; ++i )
                {
                    intValues[ i ] = values[ i ].intValue();
                }
                
                return getSum( intValues );
            default:
                throw new UnsupportedOperationException( "Not supported: " + operation );
        }
    }
    
    
    public RpcFuture< ? > badMethodThatReturnsSomething()
    {
        return new RpcResponse<>( "hello" );
    }
    
    
    public RpcFuture< Integer > badMethodThatReturnsWrongType()
    {
        return new RpcResponse<>( Integer.valueOf( 2 ) );
    }


    public RpcFuture< AllMortgagesResponse > getAllMortgages()
    {
        simulateDelayToProcessRequest();
        final AllMortgagesResponse result = BeanFactory.newBean( AllMortgagesResponse.class );
        result.setMortgages( CollectionFactory.toArray( Mortgage.class, m_mortgages ) );
        return new RpcResponse<>( result );
    }
    

    public RpcFuture< AllMortgagesResponse > getAllMortgagesWithBadResponse(
            final AllMortgagesResponse ignoredParam )
    {
        final AllMortgagesResponse response = getAllMortgages().getWithoutBlocking();
        for ( final Mortgage m : response.getMortgages() )
        {
            m.setMortgageId( null );
        }
        
        return new RpcResponse<>( response );
    }

    
    public RpcFuture< UUID > getMortgageFor( final UUID clientId )
    {
        simulateDelayToProcessRequest();
        UUID retval = null;
        for ( final Mortgage m : m_mortgages )
        {
            if ( CollectionFactory.toSet( m.getClientIds() ).contains( clientId ) )
            {
                if ( null != retval )
                {
                    throw new DaoException( 
                            GenericFailure.MULTIPLE_RESULTS_FOUND,
                            "Multiple mortgages found for client." );
                }
                retval = m.getMortgageId();
            }
        }
        
        return ( null == retval ) ? null : new RpcResponse<>( retval );
    }

    
    synchronized public RpcFuture< Object > addPaymentForMortgage(
            final UUID clientId, final UUID mortgageId, final int amount )
    {
        simulateDelayToProcessRequest();
        final UUID foundMortgageId = getMortgageFor( clientId ).getWithoutBlocking();
        if ( !mortgageId.equals( foundMortgageId ) )
        {
            throw new DaoException( 
                    GenericFailure.BAD_REQUEST,
                    "Client does not have the mortgage specified." );
        }
        
        for ( final Mortgage m : m_mortgages )
        {
            if ( m.getMortgageId().equals( mortgageId ) )
            {
                m.setRemaining( m.getRemaining() - amount );
            }
        }
        
        return null;
    }
    
    
    private void simulateDelayToProcessRequest()
    {
        if ( 0 < m_simulatedDelay )
        {
            TestUtil.sleep( ( m_dynamicDelay ) ? m_random.nextInt( m_simulatedDelay ) : m_simulatedDelay );
        }
        if ( null != m_callCounter )
        {
            m_callCounter.incrementAndGet();
        }
    }
    
    
    public boolean isBadMethodThatDoesNotReturnRpcFutureAndThusCannotBeCalledByRpcClient()
    {
        throw new UnsupportedOperationException( "Not implemented." );
    }


    private volatile boolean m_dynamicDelay;
    private volatile int m_simulatedDelay = 0;
    private volatile AtomicInteger m_callCounter;
    
    public final static UUID MORTGAGE1_ID = UUID.randomUUID();
    public final static UUID MORTGAGE2_ID = UUID.randomUUID();
    public final static UUID MORTGAGE3_ID = UUID.randomUUID();
    public final static UUID JOHN_ID = UUID.randomUUID();
    public final static UUID JASON_ID = UUID.randomUUID();
    public final static UUID BARRY_ID = UUID.randomUUID();
    private final List< Mortgage > m_mortgages = new ArrayList<>();
    private final SecureRandom m_random = new SecureRandom();
}
