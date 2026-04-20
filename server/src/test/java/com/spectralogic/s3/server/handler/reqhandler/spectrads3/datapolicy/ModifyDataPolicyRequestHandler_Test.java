/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class ModifyDataPolicyRequestHandler_Test 
{
    @Test
    public void testModifyDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_POLICY + "/" + dataPolicy.getName() )
            .addParameter( NameObservable.NAME, "newname" )
            .addParameter( DataPolicy.DEFAULT_BLOB_SIZE, "332" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( DataPolicy.class ).attain(
                Require.nothing() );
        assertEquals(
                "newname",
                support.getDatabaseSupport().getServiceManager().getRetriever(
                        DataPolicy.class ).attain( Require.nothing() ).getName(),
                "Shoulda updated bean."
                );
        assertEquals(
                1,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Shoulda invoked data planner rpc resource."
                 );
    }
}
