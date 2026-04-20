/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.ds3;

import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.rpc.dataplanner.TargetManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.Ds3TargetDataPolicies;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.ConstantResponseInvocationHandler;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.server.RpcResponse;

public final class GetDs3TargetDataPoliciesRequestHandler_Test 
{
    @Test
    public void testGetDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final Method method = ReflectUtil.getMethod( TargetManagementResource.class, "getDataPolicies" );
        final Ds3TargetDataPolicies constantResponse = BeanFactory.newBean( Ds3TargetDataPolicies.class );
        final DataPolicy dp1 = BeanFactory.newBean( DataPolicy.class );
        dp1.setName( "apple" );
        final DataPolicy dp2 = BeanFactory.newBean( DataPolicy.class );
        dp2.setName( "bob" );
        constantResponse.setDataPolicies( new DataPolicy [] { dp2, dp1 } );
        support.setTargetInterfaceIh( MockInvocationHandler.forMethod( 
                method,
                new ConstantResponseInvocationHandler( new RpcResponse<>( constantResponse ) ),
                null ) );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Ds3Target target1 = mockDaoDriver.createDs3Target( "A" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.DS3_TARGET_DATA_POLICIES + "/" + target1.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( dp1.getName() );
        driver.assertResponseToClientContains( dp2.getName() );
        assertTrue(
                driver.getResponseToClientAsString().indexOf( dp1.getName() )
                        < driver.getResponseToClientAsString().indexOf( dp2.getName() ),
                "Shoulda ordered data policies."
                 );
    }
}
