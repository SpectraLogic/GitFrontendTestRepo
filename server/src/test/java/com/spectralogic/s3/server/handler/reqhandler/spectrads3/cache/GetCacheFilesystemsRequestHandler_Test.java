/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.cache;


import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public final class GetCacheFilesystemsRequestHandler_Test 
{
    @Test
    public void testGetCacheFilesystems()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final CacheFilesystem bean = 
                new MockCacheFilesystemDriver( support.getDatabaseSupport() ).getFilesystem();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.CACHE_FILESYSTEM );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        driver.assertResponseToClientXPathEquals( "/Data/CacheFilesystem/Id", bean.getId().toString() );
    }
}
