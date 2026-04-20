/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.bucket;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;


import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.LogicalUsedCapacityInformation;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockAnonymousAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public final class GetBucketRequestHandler_Test
{
    @Test
    public void testGetBucketsAsAnonymousLogonNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b" );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockAnonymousAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/bucket/" + bucket.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
        
        final Method rpcMethod = ReflectUtil.getMethod( DataPlannerResource.class, "getLogicalUsedCapacity" );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                rpcMethod, 
                new InvocationHandler()
                {
                    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        final UUID [] ids = (UUID[])args[ 0 ];
                        
                        final List< UUID > sortedIds = CollectionFactory.toList( ids );
                        Collections.sort( sortedIds );
                        
                        final Map< UUID, Long > retval = new HashMap<>();
                        for ( final UUID id : sortedIds )
                        {
                            retval.put( id, Long.valueOf( retval.size() ) );
                        }
                        
                        final long [] array = new long[ retval.size() ];
                        int index = -1;
                        for ( final UUID id : ids )
                        {
                            array[ ++index ] = retval.get( id ).longValue();
                        }
                        
                        final LogicalUsedCapacityInformation response =
                                BeanFactory.newBean( LogicalUsedCapacityInformation.class ).setCapacities( 
                                        array );
                        return new RpcResponse<>( response );
                    }
                },
                null ) );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/bucket/" + bucket.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "<Empty>true</Empty>" );
    }
    
    
    @Test
    public void testGetBucketsReturnsBucketsWithLogicalCapacities()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final Method rpcMethod = ReflectUtil.getMethod( DataPlannerResource.class, "getLogicalUsedCapacity" );
        support.setPlannerInterfaceIh( MockInvocationHandler.forMethod(
                rpcMethod, 
                new InvocationHandler()
                {
                    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        final UUID [] ids = (UUID[])args[ 0 ];
                        
                        final List< UUID > sortedIds = CollectionFactory.toList( ids );
                        Collections.sort( sortedIds );
                        
                        final Map< UUID, Long > retval = new HashMap<>();
                        for ( final UUID id : sortedIds )
                        {
                            retval.put( id, Long.valueOf( retval.size() ) );
                        }
                        
                        final long [] array = new long[ retval.size() ];
                        int index = -1;
                        for ( final UUID id : ids )
                        {
                            array[ ++index ] = retval.get( id ).longValue();
                        }
                        
                        final LogicalUsedCapacityInformation response =
                                BeanFactory.newBean( LogicalUsedCapacityInformation.class ).setCapacities( 
                                        array );
                        return new RpcResponse<>( response );
                    }
                },
                null ) );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user" );
        final Bucket b1 = mockDaoDriver.createBucket( user.getId(), "a" );
        mockDaoDriver.addBucketAcl( b1.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.createObject( b1.getId(), "o1" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/bucket/" + b1.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "<Empty>false</Empty>" );
        final int indexBucketA = 
                driver.getResponseToClientAsString().indexOf( "<Name>a</Name>" );
        final int indexBucketB = 
                driver.getResponseToClientAsString().indexOf( "<Name>b</Name>" );
        final int indexLogicalCapacity0 = 
                driver.getResponseToClientAsString().indexOf(
                        "<LogicalUsedCapacity>0</LogicalUsedCapacity>" );
        final int indexLogicalCapacity1 = 
                driver.getResponseToClientAsString().indexOf(
                        "<LogicalUsedCapacity>1</LogicalUsedCapacity>" );
        assertTrue(
                0 <= indexBucketA,
                "Bucket a shoulda been only bucket in response."
                );
        assertTrue(
                0 > indexBucketB,
                "Bucket a shoulda been only bucket in response."
                 );
        assertTrue(
                0 <= indexLogicalCapacity0,
                "Bucket a shoulda been only bucket in response."
                 );
        assertTrue(
                0 > indexLogicalCapacity1,
                "Bucket a shoulda been only bucket in response."
                 );
    }
    
    
    @Test
    public void testGetBucketWhenNoAccessToDoSoNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user" );
        final Bucket b1 = mockDaoDriver.createBucket( user.getId(), "a" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/bucket/" + b1.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
}
