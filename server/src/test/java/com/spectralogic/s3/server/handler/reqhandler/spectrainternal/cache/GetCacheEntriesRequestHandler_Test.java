/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.cache;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.UUID;


import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.dao.service.ds3.NodeService;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CacheEntryInformation;
import com.spectralogic.s3.common.dao.domain.planner.CacheEntryState;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CacheFilesystemInformation;
import com.spectralogic.s3.common.rpc.dataplanner.domain.CacheInformation;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.security.ChecksumType;
import org.junit.jupiter.api.Test;

public final class GetCacheEntriesRequestHandler_Test
{
    @Test
    public void testGetDataPlannerCacheState()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final UUID nodeId = support
                .getDatabaseSupport()
                .getServiceManager()
                .getService( NodeService.class )
                .getThisNode()
                .getId();
        
        support.setPlannerInterfaceIh(
                MockInvocationHandler.forMethod(
                        ReflectUtil.getMethod( DataPlannerResource.class, "getCacheState" ),
                        new GetCacheStateInvocationHandler( nodeId ),
                        null ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET,
                "_rest_/cache_state" ).addParameter( RequestParameterType.FULL_DETAILS.toString(), "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        assertXPath( driver, "AvailableCapacityInBytes", "54321" );
        assertXPath( driver, "UnavailableCapacityInBytes", "4321" );
        assertXPath( driver, "Summary", "Hella" );
        assertXPath( driver, "TotalCapacityInBytes", "6789" );
        assertXPath( driver, "UsedCapacityInBytes", "3456" );

        assertXPath( driver, "Entries/State", CacheEntryState.IN_CACHE.toString() );
        assertXPath( driver, "Entries/Blob/ByteOffset", "10" );
        assertXPath( driver, "Entries/Blob/Checksum", "the_checksum" );
        assertXPath( driver, "Entries/Blob/ChecksumType", ChecksumType.SHA_256.toString() );
        assertXPath( driver, "Entries/Blob/Id", "438fc336-556b-11e4-8dc0-080027200702" );
        assertXPath( driver, "Entries/Blob/Length", "123" );
        assertXPath( driver, "Entries/Blob/MultiPartUploadParentBlobId", "" );
        assertXPath( driver, "Entries/Blob/ObjectId", "440622ba-556b-11e4-896c-080027200702" );

        assertXPath( driver, "CacheFilesystem/Id", "4322504e-556b-11e4-b466-080027200702" );
        assertXPath( driver, "CacheFilesystem/MaxCapacityInBytes", "12345" );
        assertXPath( driver, "CacheFilesystem/MaxPercentUtilizationOfFilesystem", "0.8" );
        assertXPath( driver, "CacheFilesystem/NodeId", nodeId.toString() );
        assertXPath( driver, "CacheFilesystem/Path", "/path/to/the/file/system" );
    }


    private static void assertXPath(
            final MockHttpRequestDriver driver,
            final String path,
            final String value )
    {
        driver.assertResponseToClientXPathEquals( "/Data/Filesystems/" + path, value );
    }
    
    
    private final class GetCacheStateInvocationHandler implements InvocationHandler
    {
        private GetCacheStateInvocationHandler( final UUID nodeId )
        {
            m_nodeId = nodeId;
        }


        @Override
        public Object invoke( final Object proxy, final Method method, final Object[] args )
                throws Throwable
        {
            final CacheFilesystem cacheFilesystem = BeanFactory.newBean( CacheFilesystem.class );
            cacheFilesystem.setId( UUID.fromString( "4322504e-556b-11e4-b466-080027200702" ) );
            cacheFilesystem.setMaxCapacityInBytes( Long.valueOf( 12345L ) );
            cacheFilesystem.setMaxPercentUtilizationOfFilesystem( Double.valueOf( 0.8 ) );
            cacheFilesystem.setNodeId( m_nodeId );
            cacheFilesystem.setPath( "/path/to/the/file/system" );
            
            final Blob blob = BeanFactory.newBean( Blob.class );
            blob.setByteOffset( 10L );
            blob.setChecksum( "the_checksum" );
            blob.setChecksumType( ChecksumType.SHA_256 );
            blob.setId( UUID.fromString( "438fc336-556b-11e4-8dc0-080027200702" ) );
            blob.setLength( 123L );
            blob.setObjectId( UUID.fromString( "440622ba-556b-11e4-896c-080027200702" ) );

            final CacheEntryInformation cacheEntryInformation =
                    BeanFactory.newBean( CacheEntryInformation.class );
            cacheEntryInformation.setBlob( blob );
            cacheEntryInformation.setState( CacheEntryState.IN_CACHE );

            final CacheFilesystemInformation cacheFilesystemInformation =
                    BeanFactory.newBean( CacheFilesystemInformation.class );
            cacheFilesystemInformation.setEntries( new CacheEntryInformation[] {
                    cacheEntryInformation
            } );
            cacheFilesystemInformation.setCacheFilesystem( cacheFilesystem );
            cacheFilesystemInformation.setAvailableCapacityInBytes( 54321L );
            cacheFilesystemInformation.setUnavailableCapacityInBytes( 4321L );
            cacheFilesystemInformation.setTotalCapacityInBytes( 6789L );
            cacheFilesystemInformation.setUsedCapacityInBytes( 3456L );
            cacheFilesystemInformation.setSummary( "Hella" );

            final CacheInformation cacheInformation = BeanFactory.newBean( CacheInformation.class );
            cacheInformation.setFilesystems( new CacheFilesystemInformation[] {
                    cacheFilesystemInformation
            } );

            return new RpcResponse<>( cacheInformation );
        }
        
        
        private final UUID m_nodeId;
    }//end inner class
}
