/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.server;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class RpcResponse_Test
{

    @Test
    public void testDefaultConstructorResultsInImmediateNullResponse()
    {
        final RpcResponse< String > response = new RpcResponse<>();
        assertTrue(
                response.isDone(),
                "Shoulda reported was done immediately."
                 );
        assertEquals(null, response.get( Timeout.LONG ), "Shoulda reported response.");
        assertEquals(null, response.get( 10, TimeUnit.HOURS ), "Shoulda reported response.");
        assertEquals(null, response.getWithoutBlocking(), "Shoulda reported response.");
    }
    
    
    @Test
    public void testSingleParamConstructorResultsInImmediateNullResponse()
    {
        final RpcResponse< String > response = new RpcResponse<>( "hi" );
        assertTrue(
                response.isDone(),
                "Shoulda reported was done immediately."
                 );
        assertEquals("hi", response.get( Timeout.LONG ), "Shoulda reported response.");
        assertEquals("hi", response.get( 10, TimeUnit.HOURS ), "Shoulda reported response.");
        assertEquals("hi", response.getWithoutBlocking(), "Shoulda reported response.");
    }
}
