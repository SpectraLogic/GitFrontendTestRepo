/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.testfrmwrk.target;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.ds3.DataPathBackendService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.target.PublicCloudBucketSupport;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectMetadataKeyValue;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.target.BucketOnPublicCloud;
import com.spectralogic.s3.common.rpc.target.DataOfflineablePublicCloudConnection;
import com.spectralogic.s3.common.rpc.target.PublicCloudBucketInformation;
import com.spectralogic.s3.common.rpc.target.PublicCloudConnectionFactory;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.marshal.JsonMarshaler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.TestUtil;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public abstract class MockPublicCloudConnection< CF extends PublicCloudConnectionFactory< ?, ? > >
    extends BaseShutdownable
    implements DataOfflineablePublicCloudConnection
{
    protected MockPublicCloudConnection( 
            final Class< CF > connectionFactoryType,
            final BeansServiceManager serviceManager,
            final List< BucketOnPublicCloud > segments )
    {
        m_connectionFactoryType = connectionFactoryType;
        m_ownerId = serviceManager.getService( DataPathBackendService.class )
                .attain( Require.nothing() ).getInstanceId();
        m_segments = new ArrayList<>( segments );
    }
    
    
    public void expectTakeOwnershipCalls()
    {
        m_expectTakeOwnershipCalls = true;
    }
    
    
    public void expectDeleteBucketCalls()
    {
        m_expectDeleteBucketCalls = true;
    }
    
    
    public void expectCreateBucketCalls()
    {
        m_expectCreateBucketCalls = true;
    }
    
    
    public void verifyConnectivity()
    {
        ++m_callCountIllegal;
        throw new UnsupportedOperationException( "This method should notta been invoked." );
    }
    
    
    public void cloudBucketDoesNotExist()
    {
        m_cloudBucketDoesNotExist = true; 
    }

    
    public PublicCloudBucketInformation getExistingBucketInformation( final String bucketName )
    {
        TestUtil.sleep( m_simulatedDelay );
        ++m_callCountGetExistingBucketInformation;
        if ( m_cloudBucketDoesNotExist )
        {
            return null;
        }
        
        return BeanFactory.newBean( PublicCloudBucketInformation.class )
                .setLocalBucketName( bucketName )
                .setName( bucketName )
                .setOwnerId( m_ownerId )
                .setVersion( PublicCloudBucketSupport.CLOUD_VERSION );
    }

    
    public PublicCloudBucketInformation createOrTakeoverBucket(
            final Object initialDataPlacement,
            final PublicCloudBucketInformation cloudBucket )
    {
        if ( m_expectCreateBucketCalls )
        {
            ++m_createBucketCallCount;
            return null;
        }
        ++m_callCountIllegal;
        throw new UnsupportedOperationException( "This method should notta been invoked." );
    }

    
    public PublicCloudBucketInformation takeOwnership(
            final PublicCloudBucketInformation cloudBucket,
            final UUID newOwnerId )
    {
        if ( m_expectTakeOwnershipCalls )
        {
            ++m_takeOwnershipCallCount;
            m_ownerId = newOwnerId;
            return getExistingBucketInformation( cloudBucket.getName() );
        }
        
        ++m_callCountIllegal;
        throw new UnsupportedOperationException( "This method should notta been invoked." );
    }

    
    public List<Future<?>> readBlobFromCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Blob blob,
            final File fileInCache )
    {
        ++m_callCountIllegal;
        throw new UnsupportedOperationException( "This method should notta been invoked." );
    }

    
    public List<Future<?>> writeBlobToCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final long objectSize,
            final Blob blob,
            final int numBlobsForObject,
            final File fileInCache,
            final Date objectCreationDate,
            final Set< S3ObjectProperty > metadata,
            final long maxBlobPartLength,
            final Object initialDataPlacement )
    {
        ++m_callCountIllegal;
        throw new UnsupportedOperationException( "This method should notta been invoked." );
    }

    
    public BucketOnPublicCloud discoverContents(
            final PublicCloudBucketInformation cloudBucket,
            final String marker )
    {
        return m_segments.remove( 0 );
    }

    
    public void delete( final PublicCloudBucketInformation cloudBucket, final Set< S3ObjectOnMedia > objectIds )
    {
        m_deletedObjectIds.add( BeanUtils.toMap( objectIds ).keySet() );
    }

    
    public void deleteBucket( final String bucketName )
    {
        if ( m_expectDeleteBucketCalls )
        {
            ++m_deleteBucketCallCount;
            return;
        }
        
        ++m_callCountIllegal;
        throw new UnsupportedOperationException( "This method should notta been invoked." );
    }
    
    
    public void assertTakeOwnershipCallCountEquals( final int expected )
    {
        assertEquals(
                expected,
                m_takeOwnershipCallCount,
                "Shoulda called takeOwnership expected number of times."
                 );
    }
    
    
    public void assertDeleteBucketCallCountEquals( final int expected )
    {
        assertEquals(
                expected,
                m_deleteBucketCallCount,
                "Shoulda called deleteBucket expected number of times."
                 );
    }
    
    
    public void assertCreateBucketCallCountEquals( final int expected )
    {
        assertEquals(
                expected,
                m_createBucketCallCount,
                "Shoulda called createBucket expected number of times."
                );
    }
    
    
    public void assertNoDeletesRequested( final boolean expectedVerifyBucketCall )
    {
        assertEquals(
                0,
                m_callCountIllegal,
                "Should notta invoked any unexpected methods."
                );
        assertTrue(
                0 < m_callCountConnect,
                "Shoulda connected at least once."
                 );
        assertEquals(
                expectedVerifyBucketCall,
                0 < m_callCountGetExistingBucketInformation,
                "Shoulda verified bucket at least once."
               );
        assertEquals(
                0,
                m_deletedObjectIds.size(),
                "Should notta requested any deletes."
                 );
    }
    
    
    public void assertDeletesRequestedCorrectly( final Set< UUID > expectedObjectIdsDeleted )
    {
        assertEquals(
                0,
                m_callCountIllegal,
                "Should notta invoked any unexpected methods."
                );
        assertTrue(
                0 < m_callCountConnect,
                "Shoulda connected at least once."
                 );
        assertTrue(
                0 < m_callCountGetExistingBucketInformation,
                "Shoulda verified bucket at least once."
                );
        assertTrue(
                m_segments.isEmpty(),
                "Shoulda consumed all segments."
                 );
        if ( expectedObjectIdsDeleted.isEmpty() )
        {
            assertEquals(
                    0,
                    m_deletedObjectIds.size(),
                    "Should notta deleted any objects."
                    );
            return;
        }
        assertEquals(
                1,
                m_deletedObjectIds.size(),
                "Shoulda deleted all objects in one fell swoop."
                );
        assertEquals(
                expectedObjectIdsDeleted,
                m_deletedObjectIds.get( 0 ),
                "Shoulda deleted expected objects."
                 );
    }
    
    
    public CF toConnectionFactory()
    {
        return InterfaceProxyFactory.getProxy( 
                m_connectionFactoryType,
                new InvocationHandler()
                {
                    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
                    {
                        ++m_callCountConnect;
                        return MockPublicCloudConnection.this;
                    }
                } );
    }
    
    
    public boolean isBlobReadyToBeReadFromCloud(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Blob blob )
    {
        ++m_callCountIllegal;
        throw new UnsupportedOperationException( "This method should notta been invoked." );
    }


    public void beginStagingToRead(
            final PublicCloudBucketInformation cloudBucket,
            final S3Object object,
            final Blob blob,
            final int stagedDataExpirationInDays )
    {
        ++m_callCountIllegal;
        throw new UnsupportedOperationException( "This method should notta been invoked." );
    }
    
    
    public void setOwnerId( final UUID ownerId )
    {
        m_ownerId = ownerId;
    }
    
    
    public void setSimulatedDelay( final int millis )
    {
        m_simulatedDelay = millis;
    }
    
    
    static < T extends DatabasePersistable & PublicCloudReplicationTarget< ? > > 
    List< BucketOnPublicCloud > createDiscoverableSegments( 
            final DatabaseSupport dbSupport,
            final T target,
            final int objectCount )
    {
        int checksumValue = 1000;
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Map< S3Object, List< Blob > > objects = new HashMap<>();
        for ( int i = 1; i <= objectCount; ++i )
        {
            final S3Object o = mockDaoDriver.createObject( null, "o" + i, -1 );
            final List< Blob > blobs = mockDaoDriver.createBlobs( o.getId(), i, 10 );
            for ( final Blob blob : blobs )
            {
                mockDaoDriver.updateBean(
                        blob.setChecksum( String.valueOf( ++checksumValue ) )
                            .setChecksumType( ChecksumType.MD5 ),
                        ChecksumObservable.CHECKSUM, ChecksumObservable.CHECKSUM_TYPE );
            }
            objects.put( o, blobs );
        }

        long creationDate = 0;
        final List< BucketOnPublicCloud > retval = new ArrayList<>();
        for ( final S3Object o : objects.keySet() )
        {
            final List< BlobOnMedia > boms = new ArrayList<>();
            for ( final Blob blob : objects.get( o ) )
            {
                if ( AzureTarget.class.isAssignableFrom( target.getClass() ) )
                {
                    mockDaoDriver.putBlobOnAzureTarget( target.getId(), blob.getId() );
                }
                else if ( S3Target.class.isAssignableFrom( target.getClass() ) )
                {
                    mockDaoDriver.putBlobOnS3Target( target.getId(), blob.getId() );
                }
                else
                {
                    throw new UnsupportedOperationException( "No code to support " + target.getClass() );
                }
                final BlobOnMedia bom = BeanFactory.newBean( BlobOnMedia.class );
                BeanCopier.copy( bom, blob );
                bom.setOffset( blob.getByteOffset() );
                boms.add( bom );
            }
            
            final S3ObjectOnMedia oom = BeanFactory.newBean( S3ObjectOnMedia.class );
            BeanCopier.copy( oom, o );
            oom.setObjectName( o.getName() );
            oom.setBlobs( CollectionFactory.toArray( BlobOnMedia.class, boms ) );
            
            final List< S3ObjectMetadataKeyValue > metadatas = new ArrayList<>();
            if ( 0 == creationDate )
            {
                metadatas.add( 
                    BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                    .setKey( "somekey" )
                    .setValue( "somevalue" ) );
                metadatas.add( 
                        BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                        .setKey( S3HeaderType.ETAG.getHttpHeaderName() )
                        .setValue( "etagvalue" ) );
            }
            metadatas.add(
                    BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                    .setKey( KeyValueObservable.CREATION_DATE )
                    .setValue( String.valueOf( ++creationDate ) ) );
            metadatas.add(
                    BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                    .setKey( KeyValueObservable.TOTAL_BLOB_COUNT )
                    .setValue( String.valueOf( boms.size() ) ) );
            oom.setMetadata( CollectionFactory.toArray( S3ObjectMetadataKeyValue.class, metadatas ) );
            
            final BucketOnPublicCloud bucket = BeanFactory.newBean( BucketOnPublicCloud.class );
            bucket.setBucketName( "blah" );
            bucket.setNextMarker( "blah" );
            bucket.setObjects( new S3ObjectOnMedia [] { oom } );
            retval.add( bucket );
        }
        retval.get( retval.size() - 1 ).setNextMarker( null );
        
        for ( int i = 0; i < retval.size(); ++i )
        {
            final BucketOnPublicCloud segment = retval.get( i );
            LOG.info( "Segment #" + i + ":" + Platform.NEWLINE 
                      + JsonMarshaler.formatPretty( segment.toJson() ) );
        }
        
        return retval;
    }


    public void syncUploads( final String cloudBucketName )
    {
    	//nothing to sync for this connection type
    }
    
    
    private boolean m_expectTakeOwnershipCalls;
    private boolean m_expectDeleteBucketCalls;
    private boolean m_expectCreateBucketCalls;
    private boolean m_cloudBucketDoesNotExist;
    private int m_takeOwnershipCallCount;
    private int m_deleteBucketCallCount;
    private int m_createBucketCallCount;
    private int m_callCountIllegal;
    private int m_callCountConnect;
    private int m_callCountGetExistingBucketInformation;
    private UUID m_ownerId;
    private int m_simulatedDelay;
    private final List< BucketOnPublicCloud > m_segments;
    private final List< Set< UUID > > m_deletedObjectIds = new ArrayList<>();
    private final Class< CF > m_connectionFactoryType;
    
    private final static Logger LOG = Logger.getLogger( MockPublicCloudConnection.class );
}