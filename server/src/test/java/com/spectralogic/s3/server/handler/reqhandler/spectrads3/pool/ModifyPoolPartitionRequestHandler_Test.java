/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.pool;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;

public final class ModifyPoolPartitionRequestHandler_Test 
{
    @Test
    public void testModifyDoesNotDelegateToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "p1" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.POOL_PARTITION + "/" + partition1.getName() )
            .addParameter( NameObservable.NAME, "newname" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( PoolPartition.class ).attain(
                Require.nothing() );
        assertEquals(
                "newname",
                support.getDatabaseSupport().getServiceManager().getRetriever(
                        PoolPartition.class ).attain( Require.nothing() ).getName(),
                "Shoulda updated bean."
                 );
        assertEquals(
                0,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Should notta invoked data planner rpc resource."
                 );
    }
}
