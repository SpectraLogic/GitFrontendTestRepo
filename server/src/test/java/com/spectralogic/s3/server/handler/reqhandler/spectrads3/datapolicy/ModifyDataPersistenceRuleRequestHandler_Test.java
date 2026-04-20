/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;


import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class ModifyDataPersistenceRuleRequestHandler_Test 
{
    @Test
    public void testModifyTypeDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.values()[ 0 ], storageDomain.getId() );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PERSISTENCE_RULE + "/" + rule.getId().toString() )
            .addParameter( DataPersistenceRule.TYPE, DataPersistenceRuleType.values()[ 1 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PERSISTENCE_RULE + "/" + rule.getId().toString() )
            .addParameter( DataPersistenceRule.TYPE, DataPersistenceRuleType.TEMPORARY.toString() )
            .addParameter(
                    DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN,
                    "3" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertEquals(
                DataPersistenceRuleType.values()[ 1 ],
                support.getDatabaseSupport().getServiceManager().getRetriever(
                        DataPersistenceRule.class ).attain( Require.nothing() ).getType(),
                "Shoulda updated bean."
                );
        assertEquals(
                1,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Shoulda invoked data planner rpc resource."
                );
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PERSISTENCE_RULE + "/" + rule.getId().toString() )
            .addParameter(
                    DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN,
                    "4" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PERSISTENCE_RULE + "/" + rule.getId().toString() )
            .addParameter( DataPersistenceRule.TYPE, DataPersistenceRuleType.RETIRED.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
    
    
    @Test
    public void testModifyNonTypeDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.values()[ 0 ], storageDomain.getId() );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PERSISTENCE_RULE + "/" + rule.getId().toString() )
            .addParameter( DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN, "3" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PERSISTENCE_RULE + "/" + rule.getId().toString() )
            .addParameter( DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN, "3" )
            .addParameter( DataPersistenceRule.TYPE, DataPersistenceRuleType.TEMPORARY.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertEquals(
                3,
                support.getDatabaseSupport().getServiceManager().getRetriever(
                                DataPersistenceRule.class ).attain( Require.nothing() )
                        .getMinimumDaysToRetain().intValue(),
                "Shoulda updated bean."
                 );
        assertEquals(
                1,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Shoulda invoked data planner rpc resource."
                );
    }
    
    
    @Test
    public void testModifyTypeAndNonTypeDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        final DataPersistenceRule rule = mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.values()[ 0 ], storageDomain.getId() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_PERSISTENCE_RULE + "/" + rule.getId().toString() )
            .addParameter( DataPersistenceRule.TYPE, DataPersistenceRuleType.values()[ 1 ].toString() )
            .addParameter( DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN, "3" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertEquals(
                DataPersistenceRuleType.values()[ 1 ],
                support.getDatabaseSupport().getServiceManager().getRetriever(
                        DataPersistenceRule.class ).attain( Require.nothing() ).getType(),
                "Shoulda updated bean."
                );
        assertEquals(
                3,
                support.getDatabaseSupport().getServiceManager().getRetriever(
                                DataPersistenceRule.class ).attain( Require.nothing() )
                        .getMinimumDaysToRetain().intValue(),
                "Shoulda updated bean."
                 );
        assertEquals(
                1,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Shoulda invoked data planner rpc resource."
               );
    }
}
