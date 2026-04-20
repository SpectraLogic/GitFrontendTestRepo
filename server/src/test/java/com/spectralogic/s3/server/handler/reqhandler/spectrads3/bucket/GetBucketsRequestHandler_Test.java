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
import com.spectralogic.util.security.ChecksumType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public final class GetBucketsRequestHandler_Test
{
    @Test
    public void testGetBucketsAsInternalRequestReturnsAllBuckets()
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
        final Bucket b1 = mockDaoDriver.createBucket( null, "b" );
        final Bucket b2 = mockDaoDriver.createBucket( null, "a" );
        final Bucket b3 = mockDaoDriver.createBucket( null, "cantseeme" );
        mockDaoDriver.addBucketAcl( b1.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( b2.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( b3.getId(), null, user.getId(), BucketAclPermission.WRITE );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/bucket" ).addParameter( Bucket.NAME, ChecksumType.MD5.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
    
    
    @Test
    public void testGetBucketsAsAnonymousRequestNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final User user = mockDaoDriver.createUser( "user" );
        final Bucket b1 = mockDaoDriver.createBucket( null, "b" );
        final Bucket b2 = mockDaoDriver.createBucket( null, "a" );
        final Bucket b3 = mockDaoDriver.createBucket( null, "cantseeme" );
        mockDaoDriver.addBucketAcl( b1.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( b2.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( b3.getId(), null, user.getId(), BucketAclPermission.WRITE );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockAnonymousAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/bucket" ).addParameter( Bucket.NAME, ChecksumType.MD5.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 403 );
    }
    
    
    @Test
    public void testGetBucketsWithOptionalFilterParamsWorksProvidedThatOptionalFilterParamsAreValid()
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
        final Bucket b1 = mockDaoDriver.createBucket( null, "b" );
        final Bucket b2 = mockDaoDriver.createBucket( null, "a" );
        final Bucket b3 = mockDaoDriver.createBucket( null, "cantseeme" );
        mockDaoDriver.addBucketAcl( b1.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( b2.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( b3.getId(), null, user.getId(), BucketAclPermission.WRITE );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/bucket" ).addParameter( Bucket.NAME, ChecksumType.MD5.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/bucket" ).addParameter( Bucket.CREATION_DATE, ChecksumType.MD5.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/bucket" ).addParameter( Bucket.LOGICAL_USED_CAPACITY, "1111" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testGetBucketsReturnsBucketsWithLogicalCapacitiesAndEmptyPopulated()
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
        final Bucket b2 = mockDaoDriver.createBucket( null, "b" );
        final Bucket b1 = mockDaoDriver.createBucket( null, "a" );
        final Bucket b4 = mockDaoDriver.createBucket( null, "cantseeme" );
        mockDaoDriver.addBucketAcl( b1.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( b2.getId(), null, user.getId(), BucketAclPermission.LIST );
        mockDaoDriver.addBucketAcl( b4.getId(), null, user.getId(), BucketAclPermission.WRITE );
        mockDaoDriver.createObject( b2.getId(), "o2" );
        mockDaoDriver.createObject( b2.getId(), "o3" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.GET, 
                "_rest_/bucket" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "cantseeme" );
        driver.assertResponseToClientContains( "<Empty>true</Empty>" );
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
                indexBucketA < indexBucketB,
                "Bucket b shoulda been sorted after bucket a since b is alphabetically later."
                 );
        if ( 0 > b1.getId().compareTo( b2.getId() ) )
        {
            assertTrue(
                    indexLogicalCapacity0 < indexLogicalCapacity1,
                    "The smaller capacity was for the bucket with the lesser id."
                    );
        }
        else
        {
            assertTrue(
                    indexLogicalCapacity0 > indexLogicalCapacity1,
                    "The smaller capacity was for the bucket with the greater id."
                     );
        }
    }
}
