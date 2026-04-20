/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */

package com.spectralogic.s3.common.testfrmwrk;

import java.util.HashMap;
import java.util.Map;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

public class MockObjectsOnTarget
{
    
    public MockObjectsOnTarget( final MockDaoDriver mockDaoDriver, final DatabaseSupport databaseSupport )
    {
        m_mockDaoDriver = mockDaoDriver;
        m_databaseSupport = databaseSupport;
        m_dataPolicy = mockDaoDriver.createABMConfigSingleCopyOnTape();
        m_storageDomain = m_mockDaoDriver.attainOneAndOnly( StorageDomain.class );
        PoolPartition poolPartition = m_mockDaoDriver.createPoolPartition( PoolType.ONLINE, "pp1" );
        m_mockDaoDriver.addPoolPartitionToStorageDomain( m_storageDomain.getId(), poolPartition.getId() );
    }
    
    
    public void putObjectOnPool( final String poolName, final String bucketName, final String objectName )
    {
        final Blob blob = getBlob( bucketName, objectName );
        m_mockDaoDriver.putBlobOnPoolForStorageDomain( getPool( poolName ).getId(), blob.getId(),
                m_storageDomain.getId() );
    }
    
    
    public Pool getPool( final String poolName )
    {
        return pools.computeIfAbsent( poolName, k -> createPool( poolName ) );
    }
    
    
    private Pool createPool( final String poolName )
    {
        final Pool pool = m_mockDaoDriver.createPool( m_mockDaoDriver.attainOneAndOnly( PoolPartition.class )
                                                                     .getId(), PoolState.NORMAL );
        m_databaseSupport.getServiceManager()
                         .getService( PoolService.class )
                         .update( pool.setName( poolName ), NameObservable.NAME );
        return pool;
    }
    
    
    public void putObjectOnTape( final String barCode, final String bucketName, final String objectName )
    {
        final Blob blob = getBlob( bucketName, objectName );
        m_mockDaoDriver.putBlobOnTapeForStorageDomain( getTape( barCode ).getId(), blob.getId(),
                m_storageDomain.getId() );
    }
    
    
    public Blob getBlob( final String bucketName, final String objectName )
    {
        return m_mockDaoDriver.getBlobFor( getObject( bucketName, objectName ).getId() );
    }
    
    
    public Tape getTape( final String barCode )
    {
        return tapes.computeIfAbsent( barCode, k -> createTape( barCode ) );
    }
    
    
    public S3Object getObject( final String bucketName, final String objectName )
    {
        return objects.computeIfAbsent( objectName, k -> createObject( bucketName, objectName ) );
    }
    
    
    private Tape createTape( final String barCode )
    {
        final Tape tape = m_mockDaoDriver.createTape( m_mockDaoDriver.attainOneAndOnly( TapePartition.class )
                                                                     .getId(), TapeState.NORMAL );
        m_databaseSupport.getServiceManager()
                         .getService( TapeService.class )
                         .update( tape.setBarCode( barCode ), Tape.BAR_CODE );
        return tape;
    }
    
    
    private S3Object createObject( final String bucketName, final String objectName )
    {
        return m_mockDaoDriver.createObject( getBucket( bucketName ).getId(), objectName );
    }
    
    
    public Bucket getBucket( final String bucketName )
    {
        return buckets.computeIfAbsent( bucketName, k -> createBucket( bucketName ) );
    }
    
    
    private Bucket createBucket( final String bucketName )
    {
        return m_mockDaoDriver.createBucket( null, m_dataPolicy.getId(), bucketName );
    }
    
    
    private final DataPolicy m_dataPolicy;
    private final DatabaseSupport m_databaseSupport;
    private final MockDaoDriver m_mockDaoDriver;
    private final StorageDomain m_storageDomain;
    private final Map< String, Bucket > buckets = new HashMap<>();
    private final Map< String, S3Object > objects = new HashMap<>();
    private final Map< String, Pool > pools = new HashMap<>();
    private final Map< String, Tape > tapes = new HashMap<>();
}
