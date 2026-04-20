/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.cache;

import java.io.File;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class CacheUtils_Test 
{
    @Test
    public void testGetBlobIdWithNullFileNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                CacheUtils.getBlobId( null );
            }
        } );
    }


    @Test
    public void testGetBlobIdReturnsBlobIdOfPath()
    {
        final Object expected5 = UUID.fromString( "02dfb51a-62dd-11e4-8bcb-080027200702" );
        assertEquals(expected5, CacheUtils.getBlobId( new File("/foo/bar/baz/02dfb51a-62dd-11e4-8bcb-080027200702") ), "Shoulda extracted the blob id.");
        final Object expected4 = UUID.fromString( "02dfb51a-62dd-11e4-8bcb-080027200702" );
        assertEquals(expected4, CacheUtils.getBlobId( new File("/foo/bar/baz/02dfb51a-62dd-11e4-8bcb-080027200702.v") ), "Shoulda extracted the blob id.");
        final Object expected3 = UUID.fromString( "02dfb51a-62dd-11e4-8bcb-080027200702" );
        assertEquals(expected3, CacheUtils.getBlobId( new File("../baz/02dfb51a-62dd-11e4-8bcb-080027200702") ), "Shoulda extracted the blob id.");
        final Object expected2 = UUID.fromString( "02dfb51a-62dd-11e4-8bcb-080027200702" );
        assertEquals(expected2, CacheUtils.getBlobId( new File("../baz/02dfb51a-62dd-11e4-8bcb-080027200702.v") ), "Shoulda extracted the blob id.");
        final Object expected1 = UUID.fromString( "02dfb51a-62dd-11e4-8bcb-080027200702" );
        assertEquals(expected1, CacheUtils.getBlobId( new File("02dfb51a-62dd-11e4-8bcb-080027200702") ), "Shoulda extracted the blob id.");
        final Object expected = UUID.fromString( "02dfb51a-62dd-11e4-8bcb-080027200702" );
        assertEquals(expected, CacheUtils.getBlobId( new File("02dfb51a-62dd-11e4-8bcb-080027200702.v") ), "Shoulda extracted the blob id.");
    }


    @Test
    public void testGetPathWithNullRootDirectoryNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                CacheUtils.getPath( null, UUID.randomUUID() );
            }
        } );
    }
    

    @Test
    public void testGetPathWithNullObjectIdNotAllowed()
    {
        final CacheFilesystem fs = BeanFactory.newBean( CacheFilesystem.class )
                .setPath( "tmp" + Platform.FILE_SEPARATOR );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                CacheUtils.getPath( fs, null );
            }
        } );
    }
    

    @Test
    public void testGetPathDoesSo()
    {
        final CacheFilesystem fs1 = BeanFactory.newBean( CacheFilesystem.class )
                .setPath( "tmp" + Platform.FILE_SEPARATOR );
        final Object actual1 = CacheUtils.getPath( fs1, UUID.fromString( "d8ca503f-c2e1-498b-bcae-95c5366ab09b" ) );
        assertEquals("tmp" + Platform.FILE_SEPARATOR + "d8" + Platform.FILE_SEPARATOR + "ca" + Platform.FILE_SEPARATOR +
                        "d8ca503f-c2e1-498b-bcae-95c5366ab09b", actual1, "Shoulda generated proper file name.");
        final CacheFilesystem fs2 = BeanFactory.newBean( CacheFilesystem.class ).setPath( "tmp" );
        final Object actual = CacheUtils.getPath( fs2, UUID.fromString( "a58761aa-62dd-11e4-a426-080027200702" ) );
        assertEquals("tmp" + Platform.FILE_SEPARATOR + "a5" + Platform.FILE_SEPARATOR + "87" + Platform.FILE_SEPARATOR +
                        "a58761aa-62dd-11e4-a426-080027200702", actual, "Shoulda generated proper file name.");
    }
}
