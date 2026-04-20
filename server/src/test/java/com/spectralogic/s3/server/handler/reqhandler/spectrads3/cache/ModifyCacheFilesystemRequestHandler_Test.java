/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.cache;

import java.util.UUID;


import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.dao.service.ds3.NodeService;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class ModifyCacheFilesystemRequestHandler_Test 
{
    @Test
    public void testModifyNonExistantBeanNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/invalid" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
    
    
    @Test
    public void testModifyPropertyNotAllowedToModifyNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        CacheFilesystem filesystem = BeanFactory.newBean( CacheFilesystem.class )
                .setPath( "temp" ).setMaxCapacityInBytes( Long.valueOf( 22 ) )
                .setNodeId( dbSupport.getServiceManager().getService( NodeService.class )
                        .getThisNode().getId() )
                .setCacheSafetyEnabled( false );
        dbSupport.getDataManager().createBean( filesystem );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/" + filesystem.getId().toString() ).addParameter(
                        Identifiable.ID, UUID.randomUUID().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        filesystem = dbSupport.getServiceManager().getRetriever( CacheFilesystem.class ).attain(
                filesystem.getId() );
        final Object actual = filesystem.getPath();
        assertEquals("temp" + Platform.FILE_SEPARATOR, actual, "Should notta changed anything.");
        assertEquals(22,  filesystem.getMaxCapacityInBytes().intValue(), "Should notta changed anything.");
        assertEquals(null,  filesystem.getMaxPercentUtilizationOfFilesystem(), "Should notta changed anything.");
        assertEquals(false, filesystem.getCacheSafetyEnabled(), "Should notta changed anything.");
    }
    
    
    @Test
    public void testModifyWithNoBeanPropertiesToModifyDoesNothing()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        CacheFilesystem filesystem = BeanFactory.newBean( CacheFilesystem.class )
                .setPath( "temp" )
                .setNodeId( dbSupport.getServiceManager().getService( NodeService.class )
                        .getThisNode().getId() );
        dbSupport.getDataManager().createBean( filesystem );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/" + filesystem.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        filesystem = dbSupport.getServiceManager().getRetriever( CacheFilesystem.class ).attain(
                filesystem.getId() );
        final Object actual = filesystem.getPath();
        assertEquals("temp" + Platform.FILE_SEPARATOR, actual, "Should notta changed anything.");
        assertEquals(null,  filesystem.getMaxCapacityInBytes(), "Should notta changed anything.");
        assertEquals(null,  filesystem.getMaxPercentUtilizationOfFilesystem(), "Should notta changed anything.");
    }
    
    
    @Test
    public void testModifyOnlyModifiesModifiedProperties()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        CacheFilesystem filesystem = BeanFactory.newBean( CacheFilesystem.class )
                .setPath( "temp" ).setMaxCapacityInBytes( Long.valueOf( 22 ) )
                .setNodeId( dbSupport.getServiceManager().getService( NodeService.class )
                        .getThisNode().getId() );
        dbSupport.getDataManager().createBean( filesystem );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/" + filesystem.getId().toString() ).addParameter(
                        CacheFilesystem.MAX_CAPACITY_IN_BYTES, "33333333333" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        filesystem = dbSupport.getServiceManager().getRetriever( CacheFilesystem.class ).attain(
                filesystem.getId() );
        final Object actual1 = filesystem.getPath();
        assertEquals("temp" + Platform.FILE_SEPARATOR, actual1, "Shoulda modified only the properties specified as HTTP parameters.");
        assertEquals(33333333333L,  filesystem.getMaxCapacityInBytes().longValue(), "Shoulda modified only the properties specified as HTTP parameters.");
        assertEquals(null,  filesystem.getMaxPercentUtilizationOfFilesystem(), "Shoulda modified only the properties specified as HTTP parameters.");

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/" + filesystem.getId().toString() ).addParameter(
                        CacheFilesystem.MAX_CAPACITY_IN_BYTES, "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        filesystem = dbSupport.getServiceManager().getRetriever( CacheFilesystem.class ).attain(
                filesystem.getId() );
        final Object actual = filesystem.getPath();
        assertEquals("temp" + Platform.FILE_SEPARATOR, actual, "Shoulda modified only the properties specified as HTTP parameters.");
        assertEquals(null,  filesystem.getMaxCapacityInBytes(), "Shoulda modified only the properties specified as HTTP parameters.");
        assertEquals(null,  filesystem.getMaxPercentUtilizationOfFilesystem(), "Shoulda modified only the properties specified as HTTP parameters.");
    }
    
    
    @Test
    public void testModifyValidatesMaxCapacityInBytes()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final CacheFilesystem filesystem = BeanFactory.newBean( CacheFilesystem.class )
                .setPath( "temp" )
                .setNodeId( dbSupport.getServiceManager().getService( NodeService.class )
                        .getThisNode().getId() );
        dbSupport.getDataManager().createBean( filesystem );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/" + filesystem.getId().toString() ).addParameter(
                        CacheFilesystem.MAX_CAPACITY_IN_BYTES, "33" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/" + filesystem.getId().toString() ).addParameter(
                        CacheFilesystem.MAX_CAPACITY_IN_BYTES, "3333333333" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
    
    
    @Test
    public void testModifyValidatesAutoReclaimThresholds()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final CacheFilesystem filesystem = BeanFactory.newBean( CacheFilesystem.class )
                .setPath( "temp" )
                .setNodeId( dbSupport.getServiceManager().getService( NodeService.class )
                        .getThisNode().getId() );
        dbSupport.getDataManager().createBean( filesystem );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/" + filesystem.getId().toString() ).addParameter(
                        CacheFilesystem.AUTO_RECLAIM_INITIATE_THRESHOLD, "0.5" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/" + filesystem.getId().toString() ).addParameter(
                        CacheFilesystem.AUTO_RECLAIM_INITIATE_THRESHOLD, "-0.01" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/" + filesystem.getId().toString() ).addParameter(
                        CacheFilesystem.AUTO_RECLAIM_INITIATE_THRESHOLD, "1.01" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/" + filesystem.getId().toString() ).addParameter(
                        CacheFilesystem.AUTO_RECLAIM_INITIATE_THRESHOLD, "1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/" + filesystem.getId().toString() ).addParameter(
                        CacheFilesystem.AUTO_RECLAIM_INITIATE_THRESHOLD, "0.9" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/" + filesystem.getId().toString() ).addParameter(
                        CacheFilesystem.AUTO_RECLAIM_TERMINATE_THRESHOLD, "0.91" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/" + filesystem.getId().toString() ).addParameter(
                        CacheFilesystem.AUTO_RECLAIM_TERMINATE_THRESHOLD, "-0.1" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/cache_filesystem/" + filesystem.getId().toString() ).addParameter(
                        CacheFilesystem.AUTO_RECLAIM_TERMINATE_THRESHOLD, "0" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
    }
}
