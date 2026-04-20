/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.ds3target;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.UUID;

import com.spectralogic.ds3client.Ds3Client;
import com.spectralogic.ds3client.commands.spectrads3.GetActiveJobSpectraS3Request;
import com.spectralogic.ds3client.models.ActiveJob;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class ReplicationSourceIdentifierHeaderAddingDs3ClientFactory_Test
{
    @Test
    public void testBuildNullInstanceIdNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                ReplicationSourceIdentifierHeaderAddingDs3ClientFactory.build( 
                        null,
                        InterfaceProxyFactory.getProxy( Ds3Client.class, null ) );
            }
        } );
    }
    
    @Test
    public void testBuildNullDs3ClientNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                ReplicationSourceIdentifierHeaderAddingDs3ClientFactory.build( 
                        UUID.randomUUID(), 
                        null );
            }
        } );
    }
    
    @Test
    public void testBuildHappyConstruction()
    {
        ReplicationSourceIdentifierHeaderAddingDs3ClientFactory.build( 
                UUID.randomUUID(), 
                InterfaceProxyFactory.getProxy( Ds3Client.class, null ) );
    }
    
    @Test
    public void testDelegateAddsSourceInstanceIdHeaderToRequests() throws IOException
    {
        final ActiveJob job = new ActiveJob();
        final MockDs3Client mockClient = new MockDs3Client();
        mockClient.setResponse( GetActiveJobSpectraS3Request.class, job, 200, null );
        
        final Ds3Client wrappedClient = mockClient.getClient();
        final Ds3Client client = InterfaceProxyFactory.getProxy(
                Ds3Client.class, 
                MockInvocationHandler.forReturnType(
                        String.class,
                        new ConstantResponseInvocationHandler( "abc" ),
                        new InvocationHandler()
                        {
                            public Object invoke( 
                                    final Object proxy, 
                                    final Method method, 
                                    final Object[] args ) throws Throwable
                            {
                                return method.invoke( wrappedClient, args );
                            }
                        } ) );
        final UUID instanceId = UUID.randomUUID();
        final Ds3Client proxy = 
                ReplicationSourceIdentifierHeaderAddingDs3ClientFactory.build( instanceId, client );

        assertEquals("abc", proxy.toString(), "Shoulda delegated method call.");
        final GetActiveJobSpectraS3Request request = new GetActiveJobSpectraS3Request( UUID.randomUUID() );
        proxy.getActiveJobSpectraS3( request );
        assertEquals(2, request.getHeaders().size(), "Shoulda included additional header.");

        final Collection< String > values = request.getHeaders().get(
                S3HeaderType.REPLICATION_SOURCE_IDENTIFIER.getHttpHeaderName() );
        assertNotNull(values, "Shoulda included additional header.");
        assertTrue(values.iterator().next().contains( instanceId.toString() ), "Shoulda included additional header.");

        proxy.close();
    }
}
