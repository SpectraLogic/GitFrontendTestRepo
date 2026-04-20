/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.server;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.net.rpc.mockresource.MortgageResource;
import com.spectralogic.util.net.rpc.mockresource.MortgageResourceImpl;
import com.spectralogic.util.net.tcpip.TcpIpServerTestPort;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

//TODO: These tests are failing on the build server, but work locally.
// For now they are disabled in build server builds using SKIP_RPC_TESTS env variable.
@Tag("rpc-integration")
public final class RpcServerImpl_Test
{
    @Test
    public void testRegisterNullInstanceNameAllowed()
    {
        final RpcServer server = new RpcServerImpl( TcpIpServerTestPort.NEXT_PORT.getAndIncrement() );
        server.register( null, new MortgageResourceImpl() );
        server.shutdown();
    }

    @Test
    public void testRegisterNullResourceNotAllowed()
    {
        final RpcServer server = new RpcServerImpl( TcpIpServerTestPort.NEXT_PORT.getAndIncrement() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                server.register( "singleton", null );
            }
         } );
        server.shutdown();
    }

    @Test
    public void testRegisterDoesNotBlowUpRegardlessAsToWhetherAlreadyRegistered()
    {
        final RpcServer server = new RpcServerImpl( TcpIpServerTestPort.NEXT_PORT.getAndIncrement() );
        server.register( "singleton", new MortgageResourceImpl() );
        server.register( "singleton", new MortgageResourceImpl() );
        server.shutdown();
    }

    @Test
    public void testUnregisterDoesNotBlowUpIfRegistered()
    {
        final RpcServer server = new RpcServerImpl( TcpIpServerTestPort.NEXT_PORT.getAndIncrement() );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {

        public void test()
            {
                server.unregister( "singleton", MortgageResource.class );
            }
        } );
        
        server.register( "singleton", new MortgageResourceImpl() );
        server.unregister( "singleton", MortgageResource.class );
        server.shutdown();
    }

    @Test
    public void testCannotMakeAnyCallsOnceShutdown()
    {
        final RpcServer server = new RpcServerImpl( TcpIpServerTestPort.NEXT_PORT.getAndIncrement() );
        server.register( "singleton", new MortgageResourceImpl() );
        server.shutdown();

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                server.unregister( "singleton", MortgageResource.class );
            }
            } );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
                {
                    server.register( "singleton", new MortgageResourceImpl() );
                }
            } );
    }
}
