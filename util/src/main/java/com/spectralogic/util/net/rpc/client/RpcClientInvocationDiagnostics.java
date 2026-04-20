/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;

/**
 * Diagnostic infrastructure for tracking RPC invocations made by RPC clients.  This can be useful for testing
 * purposes where it is desirable to track all the RPC invocations being made and their results.
 */
public final class RpcClientInvocationDiagnostics
{
    private RpcClientInvocationDiagnostics()
    {
        // singleton
    }
    
    
    public static RpcClientInvocationDiagnostics getInstance()
    {
        return INSTANCE;
    }
    
    
    /**
     * Enables tracking of RPC client invocations for diagnostic and testing purposes.  <br><br>
     * 
     * <font color = red><b>
     * Warning: Enabling RPC client invocations degrades performance and introduces a memory leak.  Never
     * enable it in production.
     * </font></b>
     */
    public void enable()
    {
        if ( m_enabled )
        {
            synchronized ( m_btihs )
            {
                m_btihs.clear();
                LOG.info( "RPC client invocation diagnostics reset." );
            }
        }
        else
        {
            LOG.warn( "RPC client invocation diagnostics enabled.  This should never happen in production." );
            m_enabled = true;
        }
    }
    
    
    public boolean isEnabled()
    {
        return m_enabled;
    }
    
    
    void registerBtih(
            final Class< ? extends RpcResource > rpcResource,
            String rpcResourceInstanceName,
            final BasicTestsInvocationHandler btih )
    {
        if ( null != rpcResourceInstanceName )
        {
            rpcResourceInstanceName =
                    NamingConventionType.UNDERSCORED.convert( rpcResourceInstanceName );
        }
        synchronized ( m_btihs )
        {
            if ( !m_btihs.containsKey( rpcResource ) )
            {
                m_btihs.put( rpcResource, new HashMap< String, List< BasicTestsInvocationHandler > >() );
            }
            
            final Map< String, List< BasicTestsInvocationHandler > > btihs = m_btihs.get( rpcResource );
            if ( !btihs.containsKey( rpcResourceInstanceName ) )
            {
                btihs.put( rpcResourceInstanceName, new ArrayList< BasicTestsInvocationHandler >() );
            }
            
            btihs.get( rpcResourceInstanceName ).add( btih );
            LOG.info( "Registered " + BasicTestsInvocationHandler.class.getSimpleName() 
                      + " for " + rpcResource.getSimpleName() + "." + rpcResourceInstanceName + "." );
        }
    }
    
    
    public List< BasicTestsInvocationHandler > getBtihs( 
            final Class< ? extends RpcResource > rpcResource,
            String rpcResourceInstanceName )
    {
        if ( null != rpcResourceInstanceName )
        {
            rpcResourceInstanceName =
                    NamingConventionType.UNDERSCORED.convert( rpcResourceInstanceName );
        }
        synchronized ( m_btihs )
        {
            final Map< String, List< BasicTestsInvocationHandler > > btihs = m_btihs.get( rpcResource );
            if ( null == btihs )
            {
                return null;
            }
            
            final List< BasicTestsInvocationHandler > retval = btihs.get( rpcResourceInstanceName );
            if ( null == retval )
            {
                return null;
            }
            return new ArrayList<>( retval );
        }
    }
    
    
    public BasicTestsInvocationHandler getBtih(
            final Class< ? extends RpcResource > rpcResource,
            String rpcResourceInstanceName )
    {
        final List< BasicTestsInvocationHandler > retval = getBtihs( rpcResource, rpcResourceInstanceName );
        if ( null == retval )
        {
            return null;
        }
        if ( 1 != retval.size() )
        {
            throw new IllegalStateException(
                    "Multiple " + BasicTestsInvocationHandler.class.getSimpleName() 
                    + "s found for " + rpcResource.getSimpleName() + " instance " + rpcResourceInstanceName 
                    + ".  Please use the getBtihs method instead." );
        }
        
        return retval.get( 0 );
    }
    
    
    private volatile boolean m_enabled;
    private final Map< Class< ? extends RpcResource >, Map< String, List< BasicTestsInvocationHandler > > > 
        m_btihs = new HashMap<>();
    
    private final static RpcClientInvocationDiagnostics INSTANCE = new RpcClientInvocationDiagnostics();
    private final static Logger LOG = Logger.getLogger( RpcClientInvocationDiagnostics.class );
}
