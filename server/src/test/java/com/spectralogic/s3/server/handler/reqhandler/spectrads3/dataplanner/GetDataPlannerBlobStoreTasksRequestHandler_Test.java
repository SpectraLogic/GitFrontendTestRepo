/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.dataplanner;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskInformation;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTasksInformation;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.marshal.DateMarshaler;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class GetDataPlannerBlobStoreTasksRequestHandler_Test
{
    @Test
    public void testGetDataPlannerBlobStoreTasksReturnsValidResponse()
            throws SecurityException
    {
        final Method methodGetBlobStoreTasks = 
                ReflectUtil.getMethod( DataPlannerResource.class, "getBlobStoreTasksForJob" );
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final Date date = new Date();
        
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                methodGetBlobStoreTasks,
                new InvocationHandler()
                {
                    @Override
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        final BlobStoreTaskInformation blobStoreTask =
                                BeanFactory.newBean( BlobStoreTaskInformation.class );
                        blobStoreTask.setDateScheduled( date );
                        blobStoreTask.setDateStarted( date );
                        blobStoreTask.setDescription( "task_description" );
                        blobStoreTask.setId( 10L );
                        blobStoreTask.setName( "A really intriguing task." );
                        blobStoreTask.setPriority( BlobStoreTaskPriority.URGENT );
                        blobStoreTask.setState( BlobStoreTaskState.PENDING_EXECUTION );
                        blobStoreTask.setTapeId( UUID.fromString( "34e117fa-5588-11e4-8e61-080027200702" ) );

                        final BlobStoreTasksInformation blobStoreTaskResult =
                                BeanFactory.newBean( BlobStoreTasksInformation.class );
                        blobStoreTaskResult.setTasks( new BlobStoreTaskInformation[] {
                                blobStoreTask
                        } );
                        return new RpcResponse<>( blobStoreTaskResult );
                    }
                },
                null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET,
                "_rest_/blob_store_task" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertXPath( driver, "DateScheduled", DateMarshaler.marshal( date ) );
        assertXPath( driver, "DateStarted", DateMarshaler.marshal( date ) );
        assertXPath( driver, "Description", "task_description" );
        assertXPath( driver, "Id", "10" );
        assertXPath( driver, "Name", "A really intriguing task." );
        assertXPath( driver, "Priority", BlobStoreTaskPriority.URGENT.toString() );
        assertXPath( driver, "State", BlobStoreTaskState.PENDING_EXECUTION.toString() );
        assertXPath( driver, "TapeId", "34e117fa-5588-11e4-8e61-080027200702" );
    }
    
    
    @Test
    public void testGetDataPlannerBlobStoreTasksWithoutFullDetailsSendsAppropriateRequestToDataPlanner() 
            throws SecurityException
    {
        final Method methodGetBlobStoreTasks = 
                ReflectUtil.getMethod( DataPlannerResource.class, "getBlobStoreTasksForJob" );
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                methodGetBlobStoreTasks,
                new InvocationHandler()
                {
                    @Override
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        final BlobStoreTasksInformation blobStoreTaskResult =
                                BeanFactory.newBean( BlobStoreTasksInformation.class );
                        return new RpcResponse<>( blobStoreTaskResult );
                    }
                },
                null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET,
                "_rest_/blob_store_task" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodGetBlobStoreTasks, Integer.valueOf( 1 ) );
        support.getPlannerInterfaceBtih().verifyMethodInvocations( expectedCallCounts );
        assertEquals(
                CollectionFactory.toSet(
                        BlobStoreTaskState.IN_PROGRESS, BlobStoreTaskState.PENDING_EXECUTION ),
                CollectionFactory.toSet(
                        (BlobStoreTaskState[])support.getPlannerInterfaceBtih().getMethodInvokeData(
                                methodGetBlobStoreTasks ).get( 0 ).getArgs().get( 1 ) ),
                "Shoulda sent down proper request."
                );
    }
    
    
    @Test
    public void testGetDataPlannerBlobStoreTasksWithFullDetailsSendsAppropriateRequestToDataPlanner() 
            throws SecurityException
    {
        final Method methodGetBlobStoreTasks = 
                ReflectUtil.getMethod( DataPlannerResource.class, "getBlobStoreTasksForJob" );
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                methodGetBlobStoreTasks,
                new InvocationHandler()
                {
                    @Override
                    public Object invoke( final Object proxy, final Method method, final Object[] args )
                            throws Throwable
                    {
                        final BlobStoreTasksInformation blobStoreTaskResult =
                                BeanFactory.newBean( BlobStoreTasksInformation.class );
                        return new RpcResponse<>( blobStoreTaskResult );
                    }
                },
                null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET,
                "_rest_/blob_store_task" ).addParameter( RequestParameterType.FULL_DETAILS.toString(), "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        final Map< Method, Integer > expectedCallCounts = new HashMap<>();
        expectedCallCounts.put( methodGetBlobStoreTasks, Integer.valueOf( 1 ) );
        support.getPlannerInterfaceBtih().verifyMethodInvocations( expectedCallCounts );
        assertEquals(
                CollectionFactory.toSet( BlobStoreTaskState.values() ),
                CollectionFactory.toSet(
                        (BlobStoreTaskState[])support.getPlannerInterfaceBtih().getMethodInvokeData(
                                methodGetBlobStoreTasks ).get( 0 ).getArgs().get( 1 ) ),
                "Shoulda sent down proper request."
                 );
    }


    private static void assertXPath(
            final MockHttpRequestDriver driver,
            final String path,
            final String value )
    {
        driver.assertResponseToClientXPathEquals( "/Data/Tasks/" + path, value );
    }
}
