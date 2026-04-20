/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.pool;

import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.rpc.dataplanner.PoolManagementResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;

public final class DeletePermanentlyLostPoolRequestHandler_Test 
{
    @Test
    public void testDeleteDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Pool pool = mockDaoDriver.createPool();

        final Method method = 
                ReflectUtil.getMethod( PoolManagementResource.class, "deletePermanentlyLostPool" );
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        support.setPoolInterfaceIh( btih );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/pool/" + pool.getGuid() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                1,
                btih.getMethodCallCount( method ),
                "Shoulda called the delete exactly once."
                 );
        assertEquals(
                pool.getId(),
                btih.getMethodInvokeData( method ).get( 0 ).getArgs().get( 0 ),
                "Shoulda called the delete exactly once."
                 );
    }
}
