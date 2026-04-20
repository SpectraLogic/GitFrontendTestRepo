/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;

public final class DeleteTapePartitionRequestHandler_Test 
{
    @Test
    public void testtestDeleteTapePartitionThatDoesNotExistNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createTapePartition( null, "tp1" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/tape_partition/" + "invalid" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
    
    
    @Test
    public void testtestDeleteTapePartitionThatDoesExistAndServicedByDataPlannerReturnsOk()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "tp1" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/tape_partition/" + tp1.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
    }
}
