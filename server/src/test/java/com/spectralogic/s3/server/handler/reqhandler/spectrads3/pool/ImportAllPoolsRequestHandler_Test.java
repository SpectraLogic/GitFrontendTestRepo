/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.pool;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.shared.ImportPersistenceTargetDirective;
import com.spectralogic.s3.common.dao.domain.shared.ImportDirective;
import com.spectralogic.s3.common.rpc.dataplanner.PoolManagementResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MediaOperationInvocationHandler;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.MockInvocationHandler;

public final class ImportAllPoolsRequestHandler_Test 
{
    @Test
    public void testImportAllPoolsWithRequiredParamsOnlyCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createPool();
        mockDaoDriver.createPool();
        mockDaoDriver.createPool();
        
        final MediaOperationInvocationHandler importPool = new MediaOperationInvocationHandler();
        support.setPoolInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( PoolManagementResource.class, "importPool" ),
                        importPool,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/pool" )
                        .addParameter( "operation", RestOperationType.IMPORT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                CollectionFactory.toList( (UUID)null ),
                importPool.getTapeIds(),
                "Shoulda passed null into format pool exactly once."
                 );
    }
    
    
    @Test
    public void testImportAllPoolsWithAllOptionalParamsCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createUser( "abc" );
        mockDaoDriver.createPool();
        mockDaoDriver.createPool();
        mockDaoDriver.createPool();
        
        final MediaOperationInvocationHandler importPool = new MediaOperationInvocationHandler();
        support.setPoolInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( PoolManagementResource.class, "importPool" ),
                        importPool,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/pool" )
                .addParameter( "operation", RestOperationType.IMPORT.toString() )
                .addParameter( ImportDirective.USER_ID, "abc" )
                .addParameter( ImportDirective.DATA_POLICY_ID, dataPolicy.getName() )
                .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID, storageDomain.getName() )
                .addParameter( ImportPersistenceTargetDirective.VERIFY_DATA_PRIOR_TO_IMPORT, "false" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                CollectionFactory.toList( (UUID)null ),
                importPool.getTapeIds(),
                "Shoulda passed null into format pool exactly once."
                 );
    }
}
