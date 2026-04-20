/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.shared.ImportPersistenceTargetDirective;
import com.spectralogic.s3.common.dao.domain.shared.ImportDirective;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
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

public final class ImportAllTapesRequestHandler_Test 
{
    @Test
    public void testtestImportAllTapesWithRequiredParamsOnlyCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MediaOperationInvocationHandler importTape = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeManagementResource.class, "importTape" ),
                        importTape,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape" )
                        .addParameter( "operation", RestOperationType.IMPORT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        final Object expected = CollectionFactory.toList( (UUID)null );
        assertEquals(expected, importTape.getTapeIds(), "Shoulda passed null into format tape exactly once.");
    }
    
    
    @Test
    public void testtestImportAllTapesWithAllOptionalParamsCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createUser( "abc" );
        final Tape t1 = mockDaoDriver.createTape();
        final Tape t2 = mockDaoDriver.createTape();
        final Tape t3 = mockDaoDriver.createTape();
        
        final MediaOperationInvocationHandler importTape = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeManagementResource.class, "importTape" ),
                        importTape,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape" )
                .addParameter( "operation", RestOperationType.IMPORT.toString() )
                .addParameter( ImportDirective.USER_ID, "abc" )
                .addParameter( ImportDirective.DATA_POLICY_ID, dataPolicy.getName() )
                .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID, storageDomain.getName() )
                .addParameter( ImportPersistenceTargetDirective.VERIFY_DATA_PRIOR_TO_IMPORT, "false" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        driver.assertResponseToClientDoesNotContain( t1.getId().toString() );
        driver.assertResponseToClientDoesNotContain( t2.getId().toString() );
        driver.assertResponseToClientDoesNotContain( t3.getId().toString() );

        final Object expected = CollectionFactory.toList( (UUID)null );
        assertEquals(expected, importTape.getTapeIds(), "Shoulda passed null into format tape exactly once.");
    }
}
