/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.domain;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.PhysicalPlacementApiBean;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class BlobApiBeanBuilder_Test 
{

    @Test
    public void testBuildDoesSoWhenExcludeBlobsInCacheExcludePhysicalPlacement()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createObject( null, "o1", 1000 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        mockDaoDriver.createBlobs( o2.getId(), 10, 50 );
        mockDaoDriver.createObject( null, "o3", 1000 );
        
        final Set< Blob > blobs = 
                dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet();
        final BlobApiBean [] results = new BlobApiBeanBuilder( 
                null, 
                dbSupport.getServiceManager().getRetriever( S3Object.class ), 
                blobs ).build();
        assertEquals(12,  results.length, "Shoulda reported all 12 blobs.");

        final Map< String, Integer > counts = new HashMap<>();
        for ( final BlobApiBean result : results )
        {
            final int originalCount = 
                    counts.containsKey( result.getName() ) ? counts.get( result.getName() ).intValue() : 0;
            counts.put( result.getName(), Integer.valueOf( originalCount + 1 ) );
            assertEquals(null, result.getInCache(), "Should notta reported any value for being in cache.");
        }
        assertEquals(1,  counts.get("o1").intValue(), "Shoulda reported the single blob for o1.");
        assertEquals(10,  counts.get("o2").intValue(), "Shoulda reported the 10 blobs for o2.");
        assertEquals(1,  counts.get("o3").intValue(), "Shoulda reported the single blob for o3.");
        for ( final BlobApiBean bean : results )
        {
            assertNull(bean.getPhysicalPlacement(), "Should notta populated physical placement.");
            assertNull(bean.getBucket(), "Should notta populated bucket.");
        }
    }
    
    
    @Test
    public void testBuildDoesSoWhenIncludeBlobsInCacheExcludePhysicalPlacement()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createObject( null, "o1", 1000 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        mockDaoDriver.createBlobs( o2.getId(), 10, 50 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 1000 );
        
        final Set< Blob > blobs = 
                dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet();
        final BlobApiBean [] results = new BlobApiBeanBuilder(
                null, 
                dbSupport.getServiceManager().getRetriever( S3Object.class ), 
                blobs ).includeBlobCacheState( 
                CollectionFactory.toSet( mockDaoDriver.getBlobFor( o3.getId() ).getId() ) ).build();
        assertEquals(12,  results.length, "Shoulda reported all 12 blobs.");

        int blobsInCache = 0;
        final Map< String, Integer > counts = new HashMap<>();
        for ( final BlobApiBean result : results )
        {
            final int originalCount = 
                    counts.containsKey( result.getName() ) ? counts.get( result.getName() ).intValue() : 0;
            counts.put( result.getName(), Integer.valueOf( originalCount + 1 ) );

            assertNotNull(result.getInCache(), "Shoulda reported a value for whether the blob is in cache or not.");
            if ( result.getInCache().booleanValue() )
            {
                blobsInCache += 1;
            }
        }
        assertEquals(1,  blobsInCache, "Shoulda reported the single blob in cache.");
        assertEquals(1,  counts.get("o1").intValue(), "Shoulda reported the single blob for o1.");
        assertEquals(10,  counts.get("o2").intValue(), "Shoulda reported the 10 blobs for o2.");
        assertEquals(1,  counts.get("o3").intValue(), "Shoulda reported the single blob for o3.");
        for ( final BlobApiBean bean : results )
        {
            assertNull(bean.getPhysicalPlacement(), "Should notta populated physical placement.");
        }
    }
    
    
    @Test
    public void testBuildDoesSoWhenExcludeBlobsInCacheIncludePhysicalPlacement()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object objectOnTape = mockDaoDriver.createObject( null, "o1", 1000 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > o2Blobs = mockDaoDriver.createBlobs( o2.getId(), 10, 50 );
        mockDaoDriver.createObject( null, "o3", 1000 );
        final S3Object objectToBeDeleted = mockDaoDriver.createObject( null, "o4", 1000 );
        
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape1.setBarCode( "bar1" ), Tape.BAR_CODE );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape2.setBarCode( "bar2" ), Tape.BAR_CODE );
        mockDaoDriver.putBlobOnTape( 
                tape1.getId(),
                mockDaoDriver.getBlobFor( objectOnTape.getId() ).getId() );
        mockDaoDriver.putBlobOnTape(
                tape1.getId(),
                o2Blobs.get( 0 ).getId() );
        mockDaoDriver.putBlobOnTape(
                tape1.getId(),
                o2Blobs.get( 1 ).getId() );
        mockDaoDriver.putBlobOnTape(
                tape2.getId(),
                o2Blobs.get( 1 ).getId() );
        
        final Set< Blob > blobs = 
                dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet();
        final BlobApiBeanBuilder blobApiBeanBuilder = new BlobApiBeanBuilder(
                null,
                dbSupport.getServiceManager().getRetriever( S3Object.class ), 
                blobs )
        .includePhysicalPlacement( dbSupport.getServiceManager(), false );
        
        try 
        {
            final Class<?> blobApiBeanBuilderClass = blobApiBeanBuilder.getClass();
            final Field objectsField = blobApiBeanBuilderClass.getDeclaredField( "m_objects" );
            objectsField.setAccessible( true );
            @SuppressWarnings( "unchecked" )
            final Map< UUID, S3Object > objects = (Map< UUID, S3Object >) objectsField.get( 
                    blobApiBeanBuilder );
            assertTrue(objects.size() == 4, "Objects size is now 4.");
            for ( UUID uuid : objects.keySet() )
            {
                S3Object s3Object = objects.get( uuid );
                if ( s3Object.getName().equals( objectToBeDeleted.getName() ) )
                {
                    objects.remove( uuid );
                    break;
                }
            }
            objectsField.set( blobApiBeanBuilder, objects );
            assertTrue(objects.size() == 3, "Objects size is now 3.");
        }
        catch ( final Exception ex ) 
        {
            final String message = "While trying to remove an object from blobApiBeanBuilderClass, we got exception:" 
                    + ex.getMessage();
            assertTrue(false, message);
        }

        final BlobApiBean [] results = blobApiBeanBuilder.build();
        assertEquals(12,  results.length, "Shoulda reported all 12 blobs.");

        final Map< String, Integer > counts = new HashMap<>();
        for ( final BlobApiBean result : results )
        {
            final int originalCount = 
                    counts.containsKey( result.getName() ) ? counts.get( result.getName() ).intValue() : 0;
            counts.put( result.getName(), Integer.valueOf( originalCount + 1 ) );
            assertEquals(null, result.getInCache(), "Should notta reported any value for being in cache.");
        }

        assertEquals(1,  counts.get("o1").intValue(), "Shoulda reported the single blob for o1.");
        assertEquals(10,  counts.get("o2").intValue(), "Shoulda reported the 10 blobs for o2.");
        assertEquals(1,  counts.get("o3").intValue(), "Shoulda reported the single blob for o3.");
        for ( final BlobApiBean bean : results )
        {
            assertNotNull(bean.getPhysicalPlacement(), "Shoulda populated physical placement, whether or not there is physical placement.");
            final PhysicalPlacementApiBean pp = bean.getPhysicalPlacement();
            if ( "o1".equals( bean.getName() )
                    || ( "o2".equals( bean.getName() ) && 0 == bean.getOffset() ) )
            {
                assertEquals(1,  pp.getTapes().length, "Shoulda reported physical placements correctly for " + bean + ".");
                assertEquals("bar1", pp.getTapes()[ 0 ].getBarCode(), "Shoulda reported physical placements correctly for " + bean + ".");
            }
            else if ( "o2".equals( bean.getName() ) && 50 == bean.getOffset() )
            {
                assertEquals(2,  pp.getTapes().length, "Shoulda reported physical placements correctly for " + bean + ".");
                assertEquals("bar1", pp.getTapes()[ 0 ].getBarCode(), "Shoulda reported physical placements correctly for " + bean + ".");
                assertEquals("bar2", pp.getTapes()[ 1 ].getBarCode(), "Shoulda reported physical placements correctly for " + bean + ".");
            }
            else
            {
                assertEquals(0,  pp.getTapes().length, "Shoulda reported physical placements correctly for " + bean + ".");
            }
        }
    }
    
    
    @Test
    public void testBuildDoesSoWhenExcludeBlobsInCacheIncludePhysicalPlacementAndOnlyIncludeSuspectBlobs()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object objectOnTape = mockDaoDriver.createObject( null, "o1", 1000 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > o2Blobs = mockDaoDriver.createBlobs( o2.getId(), 10, 50 );
        mockDaoDriver.createObject( null, "o3", 1000 );
        
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape1.setBarCode( "bar1" ), Tape.BAR_CODE );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape2.setBarCode( "bar2" ), Tape.BAR_CODE );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape( 
                tape1.getId(),
                mockDaoDriver.getBlobFor( objectOnTape.getId() ).getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape(
                tape1.getId(),
                o2Blobs.get( 0 ).getId() ) );
        mockDaoDriver.putBlobOnTape(
                tape2.getId(),
                o2Blobs.get( 0 ).getId() );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape(
                tape1.getId(),
                o2Blobs.get( 1 ).getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape(
                tape2.getId(),
                o2Blobs.get( 1 ).getId() ) );
        
        final Set< Blob > blobs = 
                dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet();
        final BlobApiBean [] results = new BlobApiBeanBuilder(
                null,
                dbSupport.getServiceManager().getRetriever( S3Object.class ), 
                blobs )
            .includePhysicalPlacement( dbSupport.getServiceManager(), true ).build();
        assertEquals(12,  results.length, "Shoulda reported all 12 blobs.");

        final Map< String, Integer > counts = new HashMap<>();
        for ( final BlobApiBean result : results )
        {
            final int originalCount = 
                    counts.containsKey( result.getName() ) ? counts.get( result.getName() ).intValue() : 0;
            counts.put( result.getName(), Integer.valueOf( originalCount + 1 ) );
            assertEquals(null, result.getInCache(), "Should notta reported any value for being in cache.");
        }
        assertEquals(1,  counts.get("o1").intValue(), "Shoulda reported the single blob for o1.");
        assertEquals(10,  counts.get("o2").intValue(), "Shoulda reported the 10 blobs for o2.");
        assertEquals(1,  counts.get("o3").intValue(), "Shoulda reported the single blob for o3.");
        for ( final BlobApiBean bean : results )
        {
            assertNotNull(bean.getPhysicalPlacement(), "Shoulda populated physical placement, whether or not there is physical placement.");
            final PhysicalPlacementApiBean pp = bean.getPhysicalPlacement();
            if ( "o1".equals( bean.getName() )
                    || ( "o2".equals( bean.getName() ) && 0 == bean.getOffset() ) )
            {
                assertEquals(1,  pp.getTapes().length, "Shoulda reported physical placements correctly for " + bean + ".");
                assertEquals("bar1", pp.getTapes()[ 0 ].getBarCode(), "Shoulda reported physical placements correctly for " + bean + ".");
            }
            else if ( "o2".equals( bean.getName() ) && 50 == bean.getOffset() )
            {
                assertEquals(2,  pp.getTapes().length, "Shoulda reported physical placements correctly for " + bean + ".");
                assertEquals("bar1", pp.getTapes()[ 0 ].getBarCode(), "Shoulda reported physical placements correctly for " + bean + ".");
                assertEquals("bar2", pp.getTapes()[ 1 ].getBarCode(), "Shoulda reported physical placements correctly for " + bean + ".");
            }
            else
            {
                assertEquals(0,  pp.getTapes().length, "Shoulda reported physical placements correctly for " + bean + ".");
            }
        }
    }
    
    
    @Test
    public void testBuildDoesSoWhenIncludeBlobsInCacheIncludePhysicalPlacement()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object objectOnTape = mockDaoDriver.createObject( null, "o1", 1000 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        final List< Blob > o2Blobs = mockDaoDriver.createBlobs( o2.getId(), 10, 50 );
        final S3Object o3 = mockDaoDriver.createObject( null, "o3", 1000 );
        
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape1.setBarCode( "bar1" ), Tape.BAR_CODE );
        dbSupport.getServiceManager().getService( TapeService.class ).update(
                tape2.setBarCode( "bar2" ), Tape.BAR_CODE );
        mockDaoDriver.putBlobOnTape( 
                tape1.getId(),
                mockDaoDriver.getBlobFor( objectOnTape.getId() ).getId() );
        mockDaoDriver.putBlobOnTape(
                tape1.getId(),
                o2Blobs.get( 0 ).getId() );
        mockDaoDriver.putBlobOnTape(
                tape1.getId(),
                o2Blobs.get( 1 ).getId() );
        mockDaoDriver.putBlobOnTape(
                tape2.getId(),
                o2Blobs.get( 1 ).getId() );
        
        final Set< Blob > blobs = 
                dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet();
        final BlobApiBean [] results = new BlobApiBeanBuilder(
                null, 
                dbSupport.getServiceManager().getRetriever( S3Object.class ),
                blobs )
            .includeBlobCacheState(
                    CollectionFactory.toSet( mockDaoDriver.getBlobFor( o3.getId() ).getId() ) )
            .includePhysicalPlacement( dbSupport.getServiceManager(), false ).build();
        assertEquals(12,  results.length, "Shoulda reported all 12 blobs.");

        int blobsInCache = 0;
        final Map< String, Integer > counts = new HashMap<>();
        for ( final BlobApiBean result : results )
        {
            final int originalCount = 
                    counts.containsKey( result.getName() ) ? counts.get( result.getName() ).intValue() : 0;
            counts.put( result.getName(), Integer.valueOf( originalCount + 1 ) );

            assertNotNull(result.getInCache(), "Shoulda reported a value for whether the blob is in cache or not.");
            if ( result.getInCache().booleanValue() )
            {
                blobsInCache += 1;
            }
        }
        assertEquals(1,  blobsInCache, "Shoulda reported the single blob in cache.");
        assertEquals(1,  counts.get("o1").intValue(), "Shoulda reported the single blob for o1.");
        assertEquals(10,  counts.get("o2").intValue(), "Shoulda reported the 10 blobs for o2.");
        assertEquals(1,  counts.get("o3").intValue(), "Shoulda reported the single blob for o3.");
        for ( final BlobApiBean bean : results )
        {
            assertNotNull(bean.getPhysicalPlacement(), "Shoulda populated physical placement, whether or not there is physical placement.");
            final PhysicalPlacementApiBean pp = bean.getPhysicalPlacement();
            if ( "o1".equals( bean.getName() )
                    || ( "o2".equals( bean.getName() ) && 0 == bean.getOffset() ) )
            {
                assertEquals(1,  pp.getTapes().length, "Shoulda reported physical placements correctly for " + bean + ".");
                assertEquals("bar1", pp.getTapes()[ 0 ].getBarCode(), "Shoulda reported physical placements correctly for " + bean + ".");
            }
            else if ( "o2".equals( bean.getName() ) && 50 == bean.getOffset() )
            {
                assertEquals(2,  pp.getTapes().length, "Shoulda reported physical placements correctly for " + bean + ".");
                assertEquals("bar1", pp.getTapes()[ 0 ].getBarCode(), "Shoulda reported physical placements correctly for " + bean + ".");
                assertEquals("bar2", pp.getTapes()[ 1 ].getBarCode(), "Shoulda reported physical placements correctly for " + bean + ".");
            }
            else
            {
                assertEquals(0,  pp.getTapes().length, "Shoulda reported physical placements correctly for " + bean + ".");
            }
        }
    }
    
    
    @Test
    public void testBuildDoesSoWhenBucketIncludedExcludeBlobsInCacheExcludePhysicalPlacement()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createObject( null, "o1", 1000 );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", -1 );
        mockDaoDriver.createBlobs( o2.getId(), 10, 50 );
        mockDaoDriver.createObject( null, "o3", 1000 );
        
        final Set< Blob > blobs = 
                dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet();
        final BlobApiBean [] results = new BlobApiBeanBuilder( 
                dbSupport.getServiceManager().getRetriever( Bucket.class ),
                dbSupport.getServiceManager().getRetriever( S3Object.class ), 
                blobs ).build();
        assertEquals(12,  results.length, "Shoulda reported all 12 blobs.");

        final Map< String, Integer > counts = new HashMap<>();
        for ( final BlobApiBean result : results )
        {
            final int originalCount = 
                    counts.containsKey( result.getName() ) ? counts.get( result.getName() ).intValue() : 0;
            counts.put( result.getName(), Integer.valueOf( originalCount + 1 ) );
            assertEquals(null, result.getInCache(), "Should notta reported any value for being in cache.");
        }
        assertEquals(1,  counts.get("o1").intValue(), "Shoulda reported the single blob for o1.");
        assertEquals(10,  counts.get("o2").intValue(), "Shoulda reported the 10 blobs for o2.");
        assertEquals(1,  counts.get("o3").intValue(), "Shoulda reported the single blob for o3.");
        for ( final BlobApiBean bean : results )
        {
            assertNull(bean.getPhysicalPlacement(), "Should notta populated physical placement.");
            assertNotNull(bean.getBucket(), "Shoulda populated bucket.");
        }
    }
    
}
