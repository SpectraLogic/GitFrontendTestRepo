/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.client;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class RpcProxyException_Test
{
    @Test
    public void testConstructorNullFailureNotAllowed()
    {
        TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
        {
            public void test()
            {
                throw new RpcProxyException( "", (Failure)null );
            }
            } );
    }
    
    
    @Test
    public void testConstructorNullRpcProxyExceptionNotAllowed()
    {
        TestUtil.assertThrows( null, NullPointerException.class, new BlastContainer()
        {
            public void test()
                    {
                        throw new RpcProxyException( (RpcProxyException)null );
                    }
            } );
    }
    

    @Test
    public void testExceptionGeneratedFromFailureIsProperlyFormed()
    {
        final RpcProxyException ex = new RpcProxyException( "RPCD", BeanFactory.newBean( Failure.class )
                .setCode( "OOPS" ).setHttpResponseCode( 1 ).setMessage( "hiya" ) );
        assertEquals(
                "RPCD FAILED: hiya",
                ex.getMessage(),
                "Shoulda generated exception from failure."
                 );
        assertEquals(
                "OOPS",
                ex.getFailureType().getCode(),
                "Shoulda generated exception from failure."
                 );
        assertEquals(
                1,
                ex.getFailureType().getHttpResponseCode(),
                "Shoulda generated exception from failure."
                 );
    }
}
