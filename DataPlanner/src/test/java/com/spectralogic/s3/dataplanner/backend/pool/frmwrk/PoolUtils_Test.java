/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool.frmwrk;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;



import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class PoolUtils_Test 
{
    @Test
    public void testGetComplexPathDoesSo()
    {
        final Pool pool = BeanFactory.newBean( Pool.class ).setMountpoint( "root" );
        final String bucketId = "bucket1";
        final UUID objectId = UUID.randomUUID();
        final UUID blobId = UUID.randomUUID();
        assertEquals("root" + Platform.FILE_SEPARATOR + bucketId + Platform.FILE_SEPARATOR
                + objectId.toString().charAt( 0 ) + objectId.toString().charAt( 1 )
                + Platform.FILE_SEPARATOR + objectId + Platform.FILE_SEPARATOR + blobId, PoolUtils.getPath( pool, bucketId, objectId, blobId )
                         .toString(), "Shoulda constructed path correctly.");
    }
    
    
    @Test
    public void testGetComplexPathNullBlobAllowed()
    {
        final Pool pool = BeanFactory.newBean( Pool.class ).setMountpoint( "root" );
        final String bucketId = "bucket1";
        final UUID objectId = UUID.randomUUID();
        assertEquals("root" + Platform.FILE_SEPARATOR + bucketId + Platform.FILE_SEPARATOR
                + objectId.toString().charAt( 0 ) + objectId.toString().charAt( 1 )
                + Platform.FILE_SEPARATOR + objectId, PoolUtils.getPath( pool, bucketId, objectId, null )
                                                               .toString(), "Shoulda constructed path correctly.");
    }
    
    
    @Test
    public void testGetComplexPathNullBucketNotAllowed()
    {
        final Pool pool = BeanFactory.newBean( Pool.class )
                                     .setMountpoint( "root" );
        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> PoolUtils.getPath( pool, null, UUID.randomUUID(), UUID.randomUUID() ) );
    }
    
    
    @Test
    public void testGetComplexPathNullObjectWithNonNullBlobNotAllowed()
    {
        final Pool pool = BeanFactory.newBean( Pool.class )
                                     .setMountpoint( "root" );
        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> PoolUtils.getPath( pool, "bucket1", null, UUID.randomUUID() ) );
    }
    
    
    @Test
    public void testGetComplexPathNullObjectWithNullBlobAllowed()
    {
        final Pool pool = BeanFactory.newBean( Pool.class ).setMountpoint( "root" );
        final String bucketId = "bucket1";
        assertEquals("root" + Platform.FILE_SEPARATOR + bucketId, PoolUtils.getPath( pool, bucketId, null, null )
                                                                      .toString(), "Shoulda constructed path correctly.");
    }
    
    
    @Test
    public void testGetComplexPathNullPoolNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class,
                () -> PoolUtils.getPath( null, "bucket1", UUID.randomUUID(), UUID.randomUUID() ) );
    }
    
    
    @Test
    public void testGetMetadataFile()
    {
        final Path testfile = Paths.get( "foo" );
        final Path expectedFile = Paths.get( "foo.metadata" );
        assertEquals(expectedFile, PoolUtils.getMetadataFile( testfile ), "Shoulda been the same");
    }
    
    
    @Test
    public void testGetMetadataPartFile()
    {
        final Path testfile = Paths.get( "foo" );
        final Path expectedFile = Paths.get( "foo.metadata.part" );
        assertEquals(expectedFile, PoolUtils.getMetadataPartFile( testfile ), "Shoulda been the same");
    }
    
    
    @Test
    public void testGetPropsFile()
    {
        final Path testfile = Paths.get( "foo" );
        final Path expectedFile = Paths.get( "foo.props" );
        assertEquals(expectedFile, PoolUtils.getPropsFile( testfile ), "Shoulda been the same");
    }
    
    
    @Test
    public void testGetPropsPartFile()
    {
        final Path testfile = Paths.get( "foo" );
        final Path expectedFile = Paths.get( "foo.props.part" );
        assertEquals(expectedFile, PoolUtils.getPropsPartFile( testfile ), "Shoulda been the same");
    }
    
    
    @Test
    public void testIsInfoFile()
    {
        final String[] infoSuffixes = { ".props", ".props.part", ".metadata", ".metadata.part" };
        final String[] nonInfoSuffixes =
            { ".txt", ".bmp", ".asdfasdf", ".pro", ".", ".propsa", ".props.foo" };
        for ( final String suffix : infoSuffixes)
        {
            assertTrue(
                    PoolUtils.isInfoFile( Paths.get( "a" + suffix ) ),
                    "Should be info file" );
        }
        
        for ( final String suffix : nonInfoSuffixes)
        {
            assertFalse(
                    PoolUtils.isInfoFile( Paths.get( "a" + suffix ) ),
                    "Should not be info file" );
        }
    }
}