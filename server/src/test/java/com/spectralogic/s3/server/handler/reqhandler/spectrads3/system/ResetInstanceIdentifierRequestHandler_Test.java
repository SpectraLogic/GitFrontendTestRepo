/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class ResetInstanceIdentifierRequestHandler_Test 
{
    @Test
    public void testResetDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPathBackend dpb1 = mockDaoDriver.attainOneAndOnly( DataPathBackend.class );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.INSTANCE_IDENTIFIER );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final DataPathBackend dpb2 = mockDaoDriver.attainOneAndOnly( DataPathBackend.class );
        assertFalse(
                dpb1.getInstanceId().equals( dpb2.getInstanceId() ),
                "Shoulda reset instance id."
                 );
        assertTrue(
                dpb1.getId().equals( dpb2.getId() ),
                "Shoulda reset instance id."
                 );
    }
}
