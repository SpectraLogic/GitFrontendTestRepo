/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.datapolicy;


import com.spectralogic.s3.common.dao.domain.ds3.DataIsolationLevel;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
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

public final class CreateDataPersistenceRuleRequestHandler_Test 
{
    @Test
    public void testCreateTemporaryPersistenceRuleWithoutMinDaysToRetainNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.DATA_PERSISTENCE_RULE )
                .addParameter(
                        DataPersistenceRule.TYPE,
                        DataPersistenceRuleType.TEMPORARY.toString() )
                .addParameter(
                        DataPlacement.DATA_POLICY_ID,
                        dataPolicy.getId().toString() )
                .addParameter(
                        DataPersistenceRule.STORAGE_DOMAIN_ID,
                        storageDomain.getId().toString() )
                .addParameter(
                        DataPersistenceRule.ISOLATION_LEVEL,
                        DataIsolationLevel.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
        
        assertEquals(
                0,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Should notta invoked data planner rpc resource."
                 );
    }
    
    
    @Test
    public void testCreateTemporaryPersistenceRuleWithNegativeMinDaysToRetainNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.DATA_PERSISTENCE_RULE )
                .addParameter(
                        DataPersistenceRule.TYPE,
                        DataPersistenceRuleType.TEMPORARY.toString() )
                .addParameter(
                        DataPlacement.DATA_POLICY_ID,
                        dataPolicy.getId().toString() )
                .addParameter(
                        DataPersistenceRule.STORAGE_DOMAIN_ID,
                        storageDomain.getId().toString() )
                .addParameter(
                        DataPersistenceRule.ISOLATION_LEVEL,
                        DataIsolationLevel.values()[ 0 ].toString() )
                .addParameter( 
                        DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN,
                        "-1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
        
        assertEquals(
                0,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Should notta invoked data planner rpc resource."
               );
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.DATA_PERSISTENCE_RULE )
                .addParameter(
                        DataPersistenceRule.TYPE,
                        DataPersistenceRuleType.TEMPORARY.toString() )
                .addParameter(
                        DataPlacement.DATA_POLICY_ID,
                        dataPolicy.getId().toString() )
                .addParameter(
                        DataPersistenceRule.STORAGE_DOMAIN_ID,
                        storageDomain.getId().toString() )
                .addParameter(
                        DataPersistenceRule.ISOLATION_LEVEL,
                        DataIsolationLevel.values()[ 0 ].toString() )
                .addParameter( 
                        DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN,
                        "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        assertEquals(
                1,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Shoulda invoked data planner rpc resource."
                );
    }
    
    
    @Test
    public void testCreateDelegatesToDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.DATA_PERSISTENCE_RULE )
                .addParameter(
                        DataPersistenceRule.TYPE,
                        DataPersistenceRuleType.values()[ 0 ].toString() )
                .addParameter(
                        DataPlacement.DATA_POLICY_ID,
                        dataPolicy.getId().toString() )
                .addParameter(
                        DataPersistenceRule.STORAGE_DOMAIN_ID,
                        storageDomain.getId().toString() )
                .addParameter(
                        DataPersistenceRule.ISOLATION_LEVEL,
                        DataIsolationLevel.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( DataPersistenceRule.class ).attain(
                Require.nothing() );
        assertEquals(
                1,
                support.getDataPolicyBtih().getTotalCallCount(),
                "Shoulda invoked data planner rpc resource."
                 );
    }
}
