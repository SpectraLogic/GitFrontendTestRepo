/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.frmwrk.PoolUtils;
import com.spectralogic.s3.dataplanner.testfrmwrk.MockPoolPersistence;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import org.junit.After;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ThreadedBlobVerifier_Test 
{
    private void assertEqualsMod(final String message, final Object expected, final Object actual) {
        assertEquals(expected, actual, message);
    }

    @Test
    public void testConstructorNullPoolNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, () -> new ThreadedBlobVerifier( null ) );
    }
    
    
    @Test
    public void testVerifierCorrectlyIdentifiesAnyIssues() throws IOException
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final PoolPartition partition = mockDaoDriver.createPoolPartition( PoolType.NEARLINE, "pp" );
        
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        final S3Object o0 = mockDaoDriver.createObject( bucket1.getId(), "o0", 5 * 1024 );
        final S3Object o1 = mockDaoDriver.createObject( bucket1.getId(), "o1", 1024 * 1024 );
        final S3Object o2 = mockDaoDriver.createObject( bucket2.getId(), "o2", 2 * 1024 * 1024 );
        final S3Object o3 = mockDaoDriver.createObject( bucket2.getId(), "o3", -1 );
        final Blob b0 = mockDaoDriver.getBlobFor( o0.getId() );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o3.getId(), 3, 512 * 1024 );

        b0.setChecksumType( ChecksumType.CRC_32 );
        b0.setChecksum( "cT+W2Q==" );
        b1.setChecksumType( ChecksumType.CRC_32 );
        b1.setChecksum( "TGwEyQ==" );
        b2.setChecksumType( ChecksumType.CRC_32 );
        b2.setChecksum( "cT+W2Q==" );
        
        blobs3.get( 0 ).setChecksumType( ChecksumType.CRC_32 );
        blobs3.get( 0 ).setChecksum( "JGGqGA==" );
        blobs3.get( 1 ).setChecksumType( ChecksumType.CRC_32 );
        blobs3.get( 1 ).setChecksum( "JGGqGA==" );
        blobs3.get( 2 ).setChecksumType( ChecksumType.CRC_32 );
        blobs3.get( 2 ).setChecksum( "JGGqGA==" );
        
        final MockPoolPersistence pp1 = 
                new MockPoolPersistence( dbSupport, partition.getId() );
        final MockPoolPersistence pp2 = 
                new MockPoolPersistence( dbSupport, partition.getId() );
        final Pool pool1 = pp1.getPool();
        final Pool pool2 = pp2.getPool();
        pp1.create( b0, b1, b2, blobs3.get( 0 ) );
        Files.deleteIfExists( PoolUtils.getPropsFile( PoolUtils.getPath( pool1, bucket2.getName(), o3.getId(),
                blobs3.get( 0 )
                      .getId() ) ) );
        pp2.create( b0, b2, blobs3.get( 0 ), blobs3.get( 1 ), blobs3.get( 2 ) );
        blobs3.get( 1 ).setLength( blobs3.get( 1 ).getLength() - 1 );
        blobs3.get( 2 ).setLength( blobs3.get( 2 ).getLength() + 1 );
        
        final ThreadedBlobVerifier verifier1 = new ThreadedBlobVerifier( pool1 );
        final ThreadedBlobVerifier verifier2 = new ThreadedBlobVerifier( pool2 );
        verifier1.verify( bucket1, o0, b0 );
        verifier1.verify( bucket1, o1, b1 );
        verifier2.verify( bucket1, o1, b1 );
        verifier1.verify( bucket2, o2, b2 );
        verifier1.verify( bucket2, o3, blobs3.get( 0 ) );
        verifier1.verify( bucket2, o3, blobs3.get( 1 ) );
        verifier1.verify( bucket2, o3, blobs3.get( 2 ) );
        
        verifier2.verify( bucket2, o2, b2 );
        verifier2.verify( bucket2, o3, blobs3.get( 0 ) );
        verifier2.verify( bucket2, o3, blobs3.get( 1 ) );
        verifier2.verify( bucket2, o3, blobs3.get( 2 ) );
        
        final Map< UUID, String > blobs1 = verifier1.getFailures();
        assertEquals(
                CollectionFactory.toSet(
                        b0.getId(),
                        blobs3.get( 0 ).getId(), blobs3.get( 1 ).getId(), blobs3.get( 2 ).getId() ),
                blobs1.keySet(),
                "Shoulda reported failures correctly."
                );
        assertTrue(blobs1.get( b0.getId() ).contains( "checksum" ), "Shoulda reported failures correctly.");
        assertTrue(blobs1.get( blobs3.get( 0 ).getId() ).contains( "Blob props file doesn't exist" ), "Shoulda reported failures correctly.");
        assertTrue(blobs1.get( blobs3.get( 1 ).getId() ).contains( "Blob file doesn't exist" ), "Shoulda reported failures correctly.");
        assertTrue(blobs1.get( blobs3.get( 2 ).getId() ).contains( "Blob file doesn't exist" ), "Shoulda reported failures correctly.");

        final Map< UUID, String > blobs2 = verifier2.getFailures();
        assertEquals(
                CollectionFactory.toSet(
                        b1.getId(), blobs3.get( 1 ).getId(), blobs3.get( 2 ).getId() ),
                blobs2.keySet(),
                "Shoulda reported failures correctly."
                 );
        assertTrue(blobs2.get( b1.getId() ).contains( "Blob file doesn't exist" ), "Shoulda reported failures correctly.");
        assertTrue(blobs2.get( blobs3.get( 1 ).getId() ).contains(
                        "Expected 524287 bytes, but there were 524288 bytes" ), "Shoulda reported failures correctly.");
        assertTrue(blobs2.get( blobs3.get( 2 ).getId() ).contains(
                        "Expected 524289 bytes, but there were 524288 bytes" ), "Shoulda reported failures correctly.");
    }

    private static DatabaseSupport dbSupport;

    @BeforeAll
    public static void setUpDBFactory() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    }

    @AfterEach
    public void setUp() {
        dbSupport.reset();
    }
}
