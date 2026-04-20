/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.ds3;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetReadPreferenceType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;

public final class RegisterDs3TargetRequestHandler_Test 
{
    @Test
    public void testCreateDoesDelegateToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.DS3_TARGET )
                .addParameter( NameObservable.NAME, "a" )
                .addParameter( Ds3Target.ADMIN_AUTH_ID, "b" )
                .addParameter( Ds3Target.ADMIN_SECRET_KEY, "c" )
                .addParameter( Ds3Target.DATA_PATH_END_POINT, "d" )
                .addParameter( 
                        ReplicationTarget.DEFAULT_READ_PREFERENCE, 
                        TargetReadPreferenceType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        mockDaoDriver.attainOneAndOnly( Ds3Target.class );
        assertEquals(1,  support.getTargetInterfaceBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource to register new DS3 target.");
    }
}
