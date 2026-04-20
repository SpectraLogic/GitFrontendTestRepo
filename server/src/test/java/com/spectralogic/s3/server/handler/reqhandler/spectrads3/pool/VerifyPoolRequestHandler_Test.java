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
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MediaOperationInvocationHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.MockInvocationHandler;

public final class VerifyPoolRequestHandler_Test 
{
    @Test
    public void testVerifyPoolWithPriorityCallsDataPlanner()
            throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID poolId = mockDaoDriver.createPool().getId();

        final MediaOperationInvocationHandler verifyPoolIh = new MediaOperationInvocationHandler();
        support.setPoolInterfaceIh(
                MockInvocationHandler.forMethod(
                        PoolManagementResource.class.getMethod( 
                                "verifyPool", UUID.class, BlobStoreTaskPriority.class ),
                        verifyPoolIh,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.POOL.toString() + "/" + poolId.toString() )
                        .addParameter( "operation", RestOperationType.VERIFY.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertEquals(
                CollectionFactory.toList( poolId ),
                verifyPoolIh.getTapeIds(),
                "Shoulda verifyed only the expected pool id."
                 );
        assertEquals(
                CollectionFactory.toList( (BlobStoreTaskPriority)null ),
                verifyPoolIh.getPriorities(),
                "Shoulda sent down priority correctly."
                 );
    }
    
    
    @Test
    public void testVerifyPoolWithoutPriorityCallsDataPlanner()
            throws NoSuchMethodException, SecurityException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID poolId = mockDaoDriver.createPool().getId();

        final MediaOperationInvocationHandler verifyPoolIh = new MediaOperationInvocationHandler();
        support.setPoolInterfaceIh(
                MockInvocationHandler.forMethod(
                        PoolManagementResource.class.getMethod( 
                                "verifyPool", UUID.class, BlobStoreTaskPriority.class ),
                        verifyPoolIh,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.POOL.toString() + "/" + poolId.toString() )
                        .addParameter( "operation", RestOperationType.VERIFY.toString() )
                        .addParameter( BlobStoreTask.PRIORITY, BlobStoreTaskPriority.NORMAL.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertEquals(
                CollectionFactory.toList( poolId ),
                verifyPoolIh.getTapeIds(),
                "Shoulda verifyed only the expected pool id."
               );
        assertEquals(
                CollectionFactory.toList( BlobStoreTaskPriority.NORMAL ),
                verifyPoolIh.getPriorities(),
                "Shoulda sent down priority correctly."
                );
    }
}
