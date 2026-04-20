/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
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

public class DeleteObjectsRequestHandler_Test 
{
    @Test
    public void testDeleteObjectsCorrectlyReturnsMixedErrorAndSuccessResponse()
            throws IOException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucketId );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_MULTIPLE_VERSIONS ),
                DataPolicy.VERSIONING );
        final UUID[] objectIds = {
                mockDaoDriver.createObject( bucketId, "object/" ).getId(),
                mockDaoDriver.createObject( bucketId, "object/1", new Date( 2000 ) ).getId(),
                mockDaoDriver.createObject( bucketId, "object/2" ).getId() };
        mockDaoDriver.createObject( bucketId, "object/1", 20, new Date( 1000 ) );
        
        final Set< UUID > objectIdsDeletedFromDataPlanner = new HashSet<>();
        setDataPlannerHandler(
                support,
                objectIdsDeletedFromDataPlanner,
                createDeleteObjectsResult(
                        createDeleteObjectFailure( DeleteObjectFailureReason.NOT_FOUND, objectIds[1] ) ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "test_bucket_name" )
                        .addParameter( "delete", "" );
        driver.setRequestPayload( DELETE_REQUEST.getBytes( "UTF-8" ) );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final Set< UUID > expectedDeletedObjectIds = new HashSet<>( Arrays.asList( objectIds ) );
        assertEquals( expectedDeletedObjectIds,  objectIdsDeletedFromDataPlanner, "Shoulda called data planner with the same object ids as the ones in the database.");

        driver.assertResponseToClientXPathEquals( "count(/DeleteResult/Deleted)", "2" );
        driver.assertResponseToClientXPathEquals( "/DeleteResult/Deleted[1]/Key/text()", "object/" );
        driver.assertResponseToClientXPathEquals( "/DeleteResult/Deleted[2]/Key/text()", "object/2" );

        driver.assertResponseToClientXPathEquals( "count(/DeleteResult/Error)", "1" );
        driver.assertResponseToClientXPathEquals( "/DeleteResult/Error[1]/Key/text()", "object/1" );
        driver.assertResponseToClientXPathEquals( "/DeleteResult/Error[1]/Code/text()", "ObjectNotFound" );

        assertEquals( 0,  support.getPlannerInterfaceBtih().getTotalCallCount(), "Shoulda delegated to target resource.");
        assertEquals( 1,  support.getTargetInterfaceBtih().getTotalCallCount(), "Shoulda delegated to target resource.");
    }
    
    
    @Test
    public void testDeleteObjectsDelegatesToTargetManager()
            throws IOException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucketId );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_MULTIPLE_VERSIONS ),
                DataPolicy.VERSIONING );
        final UUID[] objectIds = {
                mockDaoDriver.createObject( bucketId, "object/" ).getId(),
                mockDaoDriver.createObject( bucketId, "object/1", 10, new Date( 2000 ) ).getId(),
                mockDaoDriver.createObject( bucketId, "object/2" ).getId() };
        mockDaoDriver.createObject( bucketId, "object/1", 20, new Date( 1000 ) );
        
        final Set< UUID > objectIdsDeletedFromDataPlanner = new HashSet<>();
        setDataPlannerHandler(
                support,
                objectIdsDeletedFromDataPlanner,
                createDeleteObjectsResult(
                        createDeleteObjectFailure( DeleteObjectFailureReason.NOT_FOUND, objectIds[1] ) ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "test_bucket_name" )
                        .addParameter( "delete", "" );
        driver.setRequestPayload( DELETE_REQUEST.getBytes( "UTF-8" ) );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final Set< UUID > expectedDeletedObjectIds = new HashSet<>( Arrays.asList( objectIds ) );
        assertEquals( expectedDeletedObjectIds,  objectIdsDeletedFromDataPlanner, "Shoulda called data planner with the same object ids as the ones in the database.");

        driver.assertResponseToClientXPathEquals( "count(/DeleteResult/Deleted)", "2" );
        driver.assertResponseToClientXPathEquals( "/DeleteResult/Deleted[1]/Key/text()", "object/" );
        driver.assertResponseToClientXPathEquals( "/DeleteResult/Deleted[2]/Key/text()", "object/2" );

        driver.assertResponseToClientXPathEquals( "count(/DeleteResult/Error)", "1" );
        driver.assertResponseToClientXPathEquals( "/DeleteResult/Error[1]/Key/text()", "object/1" );
        driver.assertResponseToClientXPathEquals( "/DeleteResult/Error[1]/Code/text()", "ObjectNotFound" );

        assertEquals( 0,  support.getPlannerInterfaceBtih().getTotalCallCount(), "Shoulda delegated to target manager.");
        assertEquals( 1,  support.getTargetInterfaceBtih().getTotalCallCount(), "Shoulda delegated to target manager.");
    }
    

    @Test
    public void testDeleteObjectsReturnsErrorWhenObjectNameDoesNotExist()
            throws IOException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final UUID[] objectIds = {
                mockDaoDriver.createObject( bucketId, "object/1" ).getId(),
                mockDaoDriver.createObject( bucketId, "object/2" ).getId() };
        
        final Set< UUID > objectIdsDeletedFromDataPlanner = new HashSet<>();
        setDataPlannerHandler(
                support,
                objectIdsDeletedFromDataPlanner,
                createDeleteObjectsResult() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "test_bucket_name" )
                        .addParameter( "delete", "" );
        driver.setRequestPayload( DELETE_REQUEST.getBytes( "UTF-8" ) );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final Set< UUID > expectedDeletedObjectIds = new HashSet<>( Arrays.asList( objectIds ) );
        assertEquals( expectedDeletedObjectIds,  objectIdsDeletedFromDataPlanner, "Shoulda called data planner with the same object ids as the ones in the database.");

        driver.assertResponseToClientXPathEquals( "count(/DeleteResult/Deleted)", "2" );
        driver.assertResponseToClientXPathEquals( "/DeleteResult/Deleted[1]/Key/text()", "object/1" );
        driver.assertResponseToClientXPathEquals( "/DeleteResult/Deleted[2]/Key/text()", "object/2" );
        
        driver.assertResponseToClientXPathEquals( "count(/DeleteResult/Error)", "1" );
        driver.assertResponseToClientXPathEquals( "/DeleteResult/Error[1]/Key/text()", "object/" );
        driver.assertResponseToClientXPathEquals( "/DeleteResult/Error[1]/Code/text()", "ObjectNotFound" );
    }
    

    @Test
    public void testDeleteObjectsReturnsNoDeletedEntriesWhenQuietSet()
            throws IOException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket_name" ).getId();
        final UUID[] objectIds = {
                mockDaoDriver.createObject( bucketId, "object/1" ).getId(),
                mockDaoDriver.createObject( bucketId, "object/2" ).getId() };
        
        final Set< UUID > objectIdsDeletedFromDataPlanner = new HashSet<>();
        setDataPlannerHandler(
                support,
                objectIdsDeletedFromDataPlanner,
                createDeleteObjectsResult() );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST,
                "test_bucket_name" )
                        .addParameter( "delete", "" );
        driver.setRequestPayload( DELETE_REQUEST_WITH_QUIET.getBytes( "UTF-8" ) );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final Set< UUID > expectedDeletedObjectIds = new HashSet<>( Arrays.asList( objectIds ) );
        assertEquals( expectedDeletedObjectIds,  objectIdsDeletedFromDataPlanner, "Shoulda called data planner with the same object ids as the ones in the database.");

        driver.assertResponseToClientXPathEquals( "count(/DeleteResult/Deleted)", "0" );
        
        driver.assertResponseToClientXPathEquals( "count(/DeleteResult/Error)", "1" );
        driver.assertResponseToClientXPathEquals( "/DeleteResult/Error[1]/Key/text()", "object/" );
        driver.assertResponseToClientXPathEquals( "/DeleteResult/Error[1]/Code/text()", "ObjectNotFound" );
    }
    

    private static DeleteObjectsResult createDeleteObjectsResult( final DeleteObjectFailure... failures )
    {
        final DeleteObjectsResult result = BeanFactory.newBean( DeleteObjectsResult.class );
        result.setFailures( failures );
        return result;
    }
    

    private static DeleteObjectFailure createDeleteObjectFailure(
            final DeleteObjectFailureReason reason,
            final UUID objectId )
    {
        final DeleteObjectFailure failure = BeanFactory.newBean( DeleteObjectFailure.class );
        failure.setObjectId( objectId );
        failure.setReason( reason );
        return failure;
    }

    
    private void setDataPlannerHandler(
            final MockHttpRequestSupport support,
            final Set< UUID > objectsDeletedFromDataPlanner,
            final DeleteObjectsResult deleteObjectsResult )
    {
        final MockInvocationHandler ih = MockInvocationHandler.forMethod(
                ReflectUtil.getMethod( DeleteObjectsResource.class, "deleteObjects" ),
                new InvocationHandler()
                {
                    public Object invoke(
                            final Object proxy,
                            final Method method,
                            final Object[] args ) throws Throwable
                    {
                        final UUID[] objectIds = (UUID[]) args[2];
                        for ( final UUID objectId : objectIds )
                        {
                            objectsDeletedFromDataPlanner.add( objectId );
                        }
                        return new RpcResponse<>( deleteObjectsResult );
                    }
                },
                null );
        support.setPlannerInterfaceIh( ih );
        support.setTargetInterfaceIh( ih );
    }
    

    private static final UUID RANDOM_OBJECT_ID = UUID.randomUUID();
    private static final String DELETE_REQUEST =
            "<Delete>" +
            "<Object><Key>object/</Key></Object>" +
            "<Object><Key>object/1</Key></Object>" +
            "<Object><Key>object/2</Key></Object>" +
            "</Delete>";
    private static final String DELETE_REQUEST_WITH_QUIET =
            "<Delete>" +
            "<Quiet>true</Quiet>" +
            "<Object><Key>object/</Key></Object>" +
            "<Object><Key>object/1</Key></Object>" +
            "<Object><Key>object/2</Key></Object>" +
            "</Delete>";
}
