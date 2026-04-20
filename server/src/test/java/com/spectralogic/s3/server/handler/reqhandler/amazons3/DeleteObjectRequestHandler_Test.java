/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel;
import com.spectralogic.s3.common.rpc.dataplanner.DeleteObjectsResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectFailure;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectFailureReason;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectsResult;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class DeleteObjectRequestHandler_Test 
{
    @Test
    public void testDeleteObjectReturns404WhenBucketDoesNotExist()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "test_bucket_name/test_object_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
    
    
    @Test
    public void testDeleteObjectReturns404WhenObjectDoesNotExist()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        new MockDaoDriver( support.getDatabaseSupport() )
            .createBucket( null, "test_bucket_name" ).getId();

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "test_bucket_name/test_object_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
    
    
    @Test
    public void testDeleteObjectReturns404WhenDataPlannerDoesNotFindObject()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final UUID objectId = mockDaoDriver.createObject( bucketId, "test_object_name" ).getId();
        
        setupDataPlannerDeleteObjectsHandler( support, objectId, DeleteObjectFailureReason.NOT_FOUND );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "test_bucket_name/test_object_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
    
    
    @Test
    public void testDeleteObjectReturns204WhenFolderNotEmpty()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final UUID objectId = mockDaoDriver.createObject( bucketId, "test_object_name/" ).getId();
        
        setupDataPlannerDeleteObjectsHandler( support, objectId, null );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "test_bucket_name/test_object_name/" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                0,
                support.getPlannerInterfaceBtih().getTotalCallCount(),
                "Shoulda delegated to target resource."
                 );
        assertEquals(
                1,
                support.getTargetInterfaceBtih().getTotalCallCount(),
                "Shoulda delegated to target resource."
                );
    }
    
    
    @Test
    public void testDeleteObjectDelegatesToTargetManager()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final UUID objectId = mockDaoDriver.createObject( bucketId, "test_object_name/" ).getId();
        
        setupDataPlannerDeleteObjectsHandler( support, objectId, null );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "test_bucket_name/test_object_name/" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        
        assertEquals(
                0,
                support.getPlannerInterfaceBtih().getTotalCallCount(),
                "Shoulda delegated to target manager."
                 );
        assertEquals(
                1,
                support.getTargetInterfaceBtih().getTotalCallCount(),
                "Shoulda delegated to target manager."
                );
    }
    
    
    @Test
    public void testDeleteObjectReturns204WhenObjectExists()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucketId );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_MULTIPLE_VERSIONS ),
                DataPolicy.VERSIONING );
        final UUID objectId = mockDaoDriver.createObject( bucketId, "test_object_name" ).getId();
        mockDaoDriver.createObject( bucketId, "test_object_name", 20 );

        setupDataPlannerDeleteObjectsHandler( support, objectId, null );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "test_bucket_name/test_object_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
    }

    @Test
    public void testDeleteObjectReturns404MultiVersionsDelete()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucketId );
        mockDaoDriver.updateBean(
                dataPolicy.setVersioning( VersioningLevel.KEEP_MULTIPLE_VERSIONS ),
                DataPolicy.VERSIONING );
        final UUID objectId = mockDaoDriver.createObject( bucketId, "test_object_name" ).getId();
        mockDaoDriver.createObject( bucketId, "test_object_name", 20 );

        final UUID objectId2 = mockDaoDriver.createObject( bucketId, "test_object_name" ).getId();
        mockDaoDriver.createObject( bucketId, "test_object_name", 30 );

        setupDataPlannerDeleteObjectsHandler( support, objectId, null );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "test_bucket_name/test_object_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
    }

    @Test
    public void testDeleteObjectLatestVersionExists()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucketId );
        mockDaoDriver.updateBean(
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        final UUID objectId = mockDaoDriver.createObject( bucketId, "test_object_name" ).getId();
        mockDaoDriver.createObject( bucketId, "test_object_name", 20 );
        mockDaoDriver.createObject( bucketId, "test_object_name", 20 );
        mockDaoDriver.createObject( bucketId, "test_object_name", 20 );

        setupDataPlannerDeleteObjectsHandler( support, objectId, null );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "test_bucket_name/test_object_name" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
    }


    private static void setupDataPlannerDeleteObjectsHandler(
            final MockHttpRequestSupport support,
            final UUID objectId,
            final DeleteObjectFailureReason reason )
    {
        final DeleteObjectFailure[] failures;
        if ( null != reason )
        {
            final DeleteObjectFailure deleteObjectFailure =
                    BeanFactory.newBean( DeleteObjectFailure.class );
            deleteObjectFailure.setObjectId( objectId );
            deleteObjectFailure.setReason( reason );
            failures = new DeleteObjectFailure[] { deleteObjectFailure };
        }
        else
        {
            failures = NO_DELETE_OBJECT_FAILURES;
        }
        
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( DeleteObjectsResource.class, "deleteObjects" ),
                new InvocationHandler()
                {
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        final DeleteObjectsResult deleteObjectsResult =
                                BeanFactory.newBean( DeleteObjectsResult.class );
                        deleteObjectsResult.setFailures( failures );
                        return new RpcResponse<>( deleteObjectsResult );
                    }
                },
                null );
                
        support.setPlannerInterfaceIh( ih );
        support.setTargetInterfaceIh( ih );
    }
    

    private static final DeleteObjectFailure[] NO_DELETE_OBJECT_FAILURES = new DeleteObjectFailure[0];
}
