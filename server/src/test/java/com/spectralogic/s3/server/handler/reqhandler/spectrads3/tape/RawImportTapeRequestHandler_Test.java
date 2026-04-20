/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.shared.ImportPersistenceTargetDirective;
import com.spectralogic.s3.common.dao.domain.tape.RawImportTapeDirective;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MediaOperationInvocationHandler;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.MockInvocationHandler;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class RawImportTapeRequestHandler_Test 
{
    @Test
    public void testtestImportTapeCallsDataPlannerWhenOnlyRequiredParams()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID tapeId = mockDaoDriver.createTape().getId();
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );

        final MediaOperationInvocationHandler importTape = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeManagementResource.class, "rawImportTape" ),
                        importTape,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                        .addParameter( 
                                RequestParameterType.OPERATION.toString(), 
                                RestOperationType.IMPORT.toString() )
                        .addParameter( RawImportTapeDirective.BUCKET_ID, bucket.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object expected = CollectionFactory.toList( tapeId );
        assertEquals(expected, importTape.getTapeIds(), "Shoulda formatted only the expected tape id.");
    }
    
    
    @Test
    public void testtestImportTapeCallsDataPlannerWhenAllOptionalParamsSpecified()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID tapeId = mockDaoDriver.createTape().getId();
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );

        final MediaOperationInvocationHandler importTape = new MediaOperationInvocationHandler();
        support.setTapeInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( TapeManagementResource.class, "rawImportTape" ),
                        importTape,
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                        .addParameter( 
                                RequestParameterType.OPERATION.toString(), 
                                RestOperationType.IMPORT.toString() )
                        .addParameter( 
                                RequestParameterType.TASK_PRIORITY.toString(), 
                                BlobStoreTaskPriority.LOW.toString() )
                        .addParameter( RawImportTapeDirective.BUCKET_ID, bucket.getName() )
                        .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID,
                                storageDomain.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object expected = CollectionFactory.toList( tapeId );
        assertEquals(expected, importTape.getTapeIds(), "Shoulda formatted only the expected tape id.");
    }
}
