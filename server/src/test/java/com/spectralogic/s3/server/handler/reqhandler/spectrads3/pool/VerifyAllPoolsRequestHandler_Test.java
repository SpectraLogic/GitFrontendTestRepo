/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.pool;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.rpc.dataplanner.PoolManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MediaOperationInvocationHandler;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.MockInvocationHandler;

public final class VerifyAllPoolsRequestHandler_Test 
{
    @Test
    public void testVerifyAllPoolsCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MediaOperationInvocationHandler inspectPool = new MediaOperationInvocationHandler();
        support.setPoolInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( PoolManagementResource.class, "verifyPool" ),
                        inspectPool,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/pool" )
                        .addParameter( "operation", RestOperationType.VERIFY.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                CollectionFactory.toList( (UUID)null ),
                inspectPool.getTapeIds(),
                "Shoulda passed null into Inspect pool exactly once."
                 );
        assertEquals(
                null,
                inspectPool.getPriorities().get( 0 ),
                "Shoulda inspectted only the expected pool id."
                 );
    }
    
    
    @Test
    public void testVerifyAllPoolsWithPriorityCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MediaOperationInvocationHandler inspectPool = new MediaOperationInvocationHandler();
        support.setPoolInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( PoolManagementResource.class, "verifyPool" ),
                        inspectPool,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/pool" )
                        .addParameter( "operation", RestOperationType.VERIFY.toString() )
                        .addParameter( 
                                BlobStoreTask.PRIORITY, 
                                BlobStoreTaskPriority.HIGH.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                CollectionFactory.toList( (UUID)null ),
                inspectPool.getTapeIds(),
                "Shoulda passed null into Inspect pool exactly once."
                 );
        assertEquals(
                BlobStoreTaskPriority.HIGH,
                inspectPool.getPriorities().get( 0 ),
                "Shoulda inspectted only the expected pool id."
                 );
    }
}
