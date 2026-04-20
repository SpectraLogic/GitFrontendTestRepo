/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.cache;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;


import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import org.junit.jupiter.api.Test;

public final class ForceFullCacheReclaimRequestHandler_Test 
{
    @Test
    public void testForceFullCacheReclaimDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.CACHE_FILESYSTEM ).addParameter( 
                        RequestParameterType.RECLAIM.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put( 
                ReflectUtil.getMethod( DataPlannerResource.class, "forceFullCacheReclaimNow" ),
                Integer.valueOf( 1 ) );
        support.getPlannerInterfaceBtih().verifyMethodInvocations( expectedCalls );
    }
}
