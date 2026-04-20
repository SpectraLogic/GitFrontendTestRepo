/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;


import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public final class CreateDataPolicyRequestHandler_Test
{
    @Test
    public void testCreateWithZeroLengthNameNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.DATA_POLICY )
                .addParameter(
                        NameObservable.NAME,
                        "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        assertNull(
                support.getDatabaseSupport().getServiceManager().getRetriever( DataPolicy.class ).retrieve(
                        Require.nothing() ),
                "Should notta created data policy."
                 );
    }
    
    
    @Test
    public void testCreateDoesNotDelegateToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.DATA_POLICY )
                .addParameter(
                        NameObservable.NAME,
                        "policy1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( DataPolicy.class ).attain(
                Require.nothing() );
        assertEquals(
                0,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Should notta invoked data planner rpc resource."
                );
    }
}
