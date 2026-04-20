/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.pool;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;

public final class ModifyPoolRequestHandler_Test 
{
    @Test
    public void testModifyPoolQuiescedStateOnlyAllowedForValidStateTransitions()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Pool pool = mockDaoDriver.createPool();

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/pool/" + pool.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/pool/" + pool.getId() )
                    .addParameter( 
                            Pool.QUIESCED.toLowerCase(), 
                            Quiesced.NO.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/pool/" + pool.getId() )
                    .addParameter( 
                            Pool.QUIESCED.toLowerCase(), 
                            Quiesced.YES.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/pool/" + pool.getId() )
                    .addParameter( 
                            Pool.QUIESCED.toLowerCase(), 
                            Quiesced.PENDING.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "PENDING" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/pool/" + pool.getId() )
                    .addParameter( 
                            Pool.QUIESCED.toLowerCase(), 
                            Quiesced.YES.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/pool/" + pool.getId() )
                    .addParameter( 
                            Pool.QUIESCED.toLowerCase(), 
                            Quiesced.NO.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );
    }
    
    
    @Test
    public void testModifyPartitionDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "p1" );
        final PoolPartition partition2 = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "p2" );
        final Pool pool = mockDaoDriver.createPool( partition1.getId(), PoolState.NORMAL );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.POOL + "/" + pool.getId() )
            .addParameter( 
                    Pool.PARTITION_ID,
                    partition2.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( Pool.class ).attain(
                Require.nothing() );
        final Object expected = partition2.getId();
        assertEquals(expected, support.getDatabaseSupport().getServiceManager().getRetriever(
                        Pool.class ).attain( Require.nothing() ).getPartitionId(), "Shoulda updated bean.");
        assertEquals(1,  support.getDataPolicyBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testModifyToSetPartitionIdToNullDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "p1" );
        final Pool pool = mockDaoDriver.createPool( partition1.getId(), PoolState.NORMAL );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.POOL + "/" + pool.getId() )
            .addParameter( 
                    Pool.PARTITION_ID,
                    "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( Pool.class ).attain(
                Require.nothing() );
        assertEquals(null, support.getDatabaseSupport().getServiceManager().getRetriever(
                        Pool.class ).attain( Require.nothing() ).getPartitionId(), "Shoulda updated bean.");
        assertEquals(1,  support.getDataPolicyBtih().getTotalCallCount(), "Shoulda invoked data planner rpc resource.");
    }
    
    
    @Test
    public void testModifyToSetPartitionIdToNullAsStringNullNotAllowedAndGetBadRequest()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final PoolPartition partition1 = mockDaoDriver.createPoolPartition( PoolType.values()[ 0 ], "p1" );
        final Pool pool = mockDaoDriver.createPool( partition1.getId(), PoolState.NORMAL );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.POOL + "/" + pool.getId() )
            .addParameter( 
                    Pool.PARTITION_ID,
                    "null" );
        driver.run();
        driver.assertHttpResponseCodeEquals( GenericFailure.NOT_FOUND.getHttpResponseCode() );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( Pool.class ).attain(
                Require.nothing() );
        final Object expected = partition1.getId();
        assertEquals(expected, support.getDatabaseSupport().getServiceManager().getRetriever(
                        Pool.class ).attain( Require.nothing() ).getPartitionId(), "Should notta updated bean.");
    }
}
