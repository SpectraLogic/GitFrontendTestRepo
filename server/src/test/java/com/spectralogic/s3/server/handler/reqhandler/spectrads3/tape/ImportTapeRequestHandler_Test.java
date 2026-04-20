/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.shared.ImportDirective;
import com.spectralogic.s3.common.dao.domain.shared.ImportPersistenceTargetDirective;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.ImportPersistenceTargetDirectiveRequest;
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

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class ImportTapeRequestHandler_Test 
{
    @Test
    public void testtestImportTapeWithRequiredParamsOnlyCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID tapeId = mockDaoDriver.createTape().getId();

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
                "_rest_/tape/" + tapeId.toString() )
                        .addParameter( "operation", RestOperationType.IMPORT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object expected = CollectionFactory.toList( tapeId );
        assertEquals(expected, importTape.getTapeIds(), "Shoulda formatted only the expected tape id.");

    }
    
    
    @Test
    public void testtestVerifyPriorToImportDefaultsToSystemDefaultIfNotSpecified()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createUser( "abc" );
        final UUID tapeId = mockDaoDriver.createTape().getId();
        
        mockDaoDriver.updateAllBeans( 
              mockDaoDriver.attainOneAndOnly( DataPathBackend.class ).setDefaultVerifyDataPriorToImport( 
                      false ),
              DataPathBackend.DEFAULT_VERIFY_DATA_PRIOR_TO_IMPORT );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                .addParameter( "operation", RestOperationType.IMPORT.toString() )
                .addParameter( ImportDirective.USER_ID, "abc" )
                .addParameter( ImportDirective.DATA_POLICY_ID, dataPolicy.getName() )
                .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID, storageDomain.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(false, ( (ImportPersistenceTargetDirectiveRequest)
                        support.getTapeInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                        .isVerifyDataPriorToImport(), "Shoulda defaulted to system default.");
    }
    
    
    @Test
    public void testtestVerifyPriorToImportUsesSpecifiedModeIfSpecified()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createUser( "abc" );
        final UUID tapeId = mockDaoDriver.createTape().getId();
        
        mockDaoDriver.updateAllBeans( 
                mockDaoDriver.attainOneAndOnly( DataPathBackend.class ).setDefaultVerifyDataPriorToImport( 
                        false ),
                DataPathBackend.DEFAULT_VERIFY_DATA_PRIOR_TO_IMPORT );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                .addParameter( "operation", RestOperationType.IMPORT.toString() )
                .addParameter( ImportDirective.USER_ID, "abc" )
                .addParameter( ImportDirective.DATA_POLICY_ID, dataPolicy.getName() )
                .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID, storageDomain.getName() )
                .addParameter(
                        ImportPersistenceTargetDirective.VERIFY_DATA_PRIOR_TO_IMPORT, 
                        "true" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(true, ( (ImportPersistenceTargetDirectiveRequest)
                        support.getTapeInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                        .isVerifyDataPriorToImport(), "Shoulda used specified value.");
    }
    
    
    @Test
    public void testtestVerifyAfterImportDefaultsToSystemDefaultIfNotSpecifiedWhenDefaultNonNull()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createUser( "abc" );
        final UUID tapeId = mockDaoDriver.createTape().getId();
        
        mockDaoDriver.updateAllBeans( 
                mockDaoDriver.attainOneAndOnly( DataPathBackend.class )
                .setDefaultVerifyDataPriorToImport( false )
                .setDefaultVerifyDataAfterImport( BlobStoreTaskPriority.values()[ 2 ] ),
                DataPathBackend.DEFAULT_VERIFY_DATA_PRIOR_TO_IMPORT,
                DataPathBackend.DEFAULT_VERIFY_DATA_AFTER_IMPORT );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                .addParameter( "operation", RestOperationType.IMPORT.toString() )
                .addParameter( ImportDirective.USER_ID, "abc" )
                .addParameter( ImportDirective.DATA_POLICY_ID, dataPolicy.getName() )
                .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID, storageDomain.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object actual = ( (ImportPersistenceTargetDirectiveRequest)
                support.getTapeInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                .getVerifyDataAfterImport();
        assertEquals(BlobStoreTaskPriority.values()[ 2 ], actual, "Shoulda defaulted to system default.");
    }
    
    
    @Test
    public void testtestVerifyAfterImportUsesSpecifiedModeIfSpecifiedWhenDefaultNonNull()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createUser( "abc" );
        final UUID tapeId = mockDaoDriver.createTape().getId();
        
        mockDaoDriver.updateAllBeans( 
                mockDaoDriver.attainOneAndOnly( DataPathBackend.class )
                .setDefaultVerifyDataPriorToImport( false )
                .setDefaultVerifyDataAfterImport( BlobStoreTaskPriority.values()[ 2 ] ),
                DataPathBackend.DEFAULT_VERIFY_DATA_PRIOR_TO_IMPORT,
                DataPathBackend.DEFAULT_VERIFY_DATA_AFTER_IMPORT );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                .addParameter( "operation", RestOperationType.IMPORT.toString() )
                .addParameter( ImportDirective.USER_ID, "abc" )
                .addParameter( ImportDirective.DATA_POLICY_ID, dataPolicy.getName() )
                .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID, storageDomain.getName() )
                .addParameter(
                        ImportPersistenceTargetDirective.VERIFY_DATA_AFTER_IMPORT, 
                        BlobStoreTaskPriority.values()[ 3 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object actual = ( (ImportPersistenceTargetDirectiveRequest)
                support.getTapeInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                .getVerifyDataAfterImport();
        assertEquals(BlobStoreTaskPriority.values()[ 3 ], actual, "Shoulda used specified value.");

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                .addParameter( "operation", RestOperationType.IMPORT.toString() )
                .addParameter( ImportDirective.USER_ID, "abc" )
                .addParameter( ImportDirective.DATA_POLICY_ID, dataPolicy.getName() )
                .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID, storageDomain.getName() )
                .addParameter(
                        ImportPersistenceTargetDirective.VERIFY_DATA_AFTER_IMPORT, 
                        "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertNull(( (ImportPersistenceTargetDirectiveRequest)
                        support.getTapeInterfaceBtih().getMethodInvokeData().get( 1 ).getArgs().get( 1 ) )
                        .getVerifyDataAfterImport(), "Shoulda used specified value.");
    }
    
    
    @Test
    public void testtestVerifyAfterImportDefaultsToSystemDefaultIfNotSpecifiedWhenDefaultNull()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createUser( "abc" );
        final UUID tapeId = mockDaoDriver.createTape().getId();
        
        mockDaoDriver.updateAllBeans( 
                mockDaoDriver.attainOneAndOnly( DataPathBackend.class )
                .setDefaultVerifyDataPriorToImport( false )
                .setDefaultVerifyDataAfterImport( null ),
                DataPathBackend.DEFAULT_VERIFY_DATA_PRIOR_TO_IMPORT,
                DataPathBackend.DEFAULT_VERIFY_DATA_AFTER_IMPORT );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                .addParameter( "operation", RestOperationType.IMPORT.toString() )
                .addParameter( ImportDirective.USER_ID, "abc" )
                .addParameter( ImportDirective.DATA_POLICY_ID, dataPolicy.getName() )
                .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID, storageDomain.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertNull(( (ImportPersistenceTargetDirectiveRequest)
                        support.getTapeInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                        .getVerifyDataAfterImport(), "Shoulda defaulted to system default.");
    }
    
    
    @Test
    public void testtestVerifyAfterImportUsesSpecifiedModeIfSpecifiedWhenDefaultNull()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createUser( "abc" );
        final UUID tapeId = mockDaoDriver.createTape().getId();
        
        mockDaoDriver.updateAllBeans( 
                mockDaoDriver.attainOneAndOnly( DataPathBackend.class )
                .setDefaultVerifyDataPriorToImport( false )
                .setDefaultVerifyDataAfterImport( null ),
                DataPathBackend.DEFAULT_VERIFY_DATA_PRIOR_TO_IMPORT,
                DataPathBackend.DEFAULT_VERIFY_DATA_AFTER_IMPORT );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                .addParameter( "operation", RestOperationType.IMPORT.toString() )
                .addParameter( ImportDirective.USER_ID, "abc" )
                .addParameter( ImportDirective.DATA_POLICY_ID, dataPolicy.getName() )
                .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID, storageDomain.getName() )
                .addParameter(
                        ImportPersistenceTargetDirective.VERIFY_DATA_AFTER_IMPORT, 
                        BlobStoreTaskPriority.values()[ 3 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object actual = ( (ImportPersistenceTargetDirectiveRequest)
                support.getTapeInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                .getVerifyDataAfterImport();
        assertEquals(BlobStoreTaskPriority.values()[ 3 ], actual, "Shoulda used specified value.");

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                .addParameter( "operation", RestOperationType.IMPORT.toString() )
                .addParameter( ImportDirective.USER_ID, "abc" )
                .addParameter( ImportDirective.DATA_POLICY_ID, dataPolicy.getName() )
                .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID, storageDomain.getName() )
                .addParameter(
                        ImportPersistenceTargetDirective.VERIFY_DATA_AFTER_IMPORT, 
                        "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertNull(( (ImportPersistenceTargetDirectiveRequest)
                        support.getTapeInterfaceBtih().getMethodInvokeData().get( 1 ).getArgs().get( 1 ) )
                        .getVerifyDataAfterImport(), "Shoulda used specified value.");
    }
    
    
    @Test
    public void testtestImportTapeWithAllOptionalParamsCallsDataPlanner1()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createUser( "abc" );
        final UUID tapeId = mockDaoDriver.createTape().getId();

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
                "_rest_/tape/" + tapeId.toString() )
                .addParameter( "operation", RestOperationType.IMPORT.toString() )
                .addParameter( ImportDirective.USER_ID, "abc" )
                .addParameter( ImportDirective.DATA_POLICY_ID, dataPolicy.getName() )
                .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID, storageDomain.getName() )
                .addParameter( ImportPersistenceTargetDirective.VERIFY_DATA_PRIOR_TO_IMPORT, "true" )
                .addParameter( 
                        ImportPersistenceTargetDirectiveRequest.PRIORITY, 
                        BlobStoreTaskPriority.LOW.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object expected = CollectionFactory.toList( tapeId );
        assertEquals(expected, importTape.getTapeIds(), "Shoulda formatted only the expected tape id.");
        assertEquals(BlobStoreTaskPriority.LOW, ( (ImportPersistenceTargetDirectiveRequest)
                        support.getTapeInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                        .getPriority(), "Shoulda sent down LOW priority.");
    }
    
    
    @Test
    public void testtestImportTapeWithAllOptionalParamsCallsDataPlanner2()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createUser( "abc" );
        final UUID tapeId = mockDaoDriver.createTape().getId();

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
                "_rest_/tape/" + tapeId.toString() )
                .addParameter( "operation", RestOperationType.IMPORT.toString() )
                .addParameter( ImportDirective.USER_ID, "abc" )
                .addParameter( ImportDirective.DATA_POLICY_ID, dataPolicy.getName() )
                .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID, storageDomain.getName() )
                .addParameter( ImportPersistenceTargetDirective.VERIFY_DATA_PRIOR_TO_IMPORT, "false" )
                .addParameter(
                        ImportPersistenceTargetDirective.VERIFY_DATA_AFTER_IMPORT,
                        BlobStoreTaskPriority.values()[ 1 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object expected = CollectionFactory.toList( tapeId );
        assertEquals(expected, importTape.getTapeIds(), "Shoulda formatted only the expected tape id.");
        assertEquals(BlobStoreTaskPriority.NORMAL, ( (ImportPersistenceTargetDirectiveRequest)
                        support.getTapeInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                        .getPriority(), "Shoulda sent down NORMAL priority by default.");
    }
    
    
    @Test
    public void testtestImportTapeWithDuplicateVerifyRequestsNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createUser( "abc" );
        final UUID tapeId = mockDaoDriver.createTape().getId();

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
                "_rest_/tape/" + tapeId.toString() )
                .addParameter( "operation", RestOperationType.IMPORT.toString() )
                .addParameter( ImportDirective.USER_ID, "abc" )
                .addParameter( ImportDirective.DATA_POLICY_ID, dataPolicy.getName() )
                .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID, storageDomain.getName() )
                .addParameter( ImportPersistenceTargetDirective.VERIFY_DATA_PRIOR_TO_IMPORT, "true" )
                .addParameter(
                        ImportPersistenceTargetDirective.VERIFY_DATA_AFTER_IMPORT,
                        BlobStoreTaskPriority.values()[ 1 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testtestImportTapeWithImplicitDataPolicyForUserWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.createUser( "abc" );
        mockDaoDriver.grantDataPolicyAccessToEveryUser();
        final UUID tapeId = mockDaoDriver.createTape().getId();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/tape/" + tapeId.toString() )
                .addParameter( "operation", RestOperationType.IMPORT.toString() )
                .addParameter( ImportDirective.USER_ID, "abc" )
                .addParameter( ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID, storageDomain.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        final Object expected = dataPolicy.getId();
        assertEquals(expected, ( (ImportPersistenceTargetDirectiveRequest)
                        support.getTapeInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 1 ) )
                        .getDataPolicyId(), "Shoulda formatted only the expected tape id.");
    }
}
