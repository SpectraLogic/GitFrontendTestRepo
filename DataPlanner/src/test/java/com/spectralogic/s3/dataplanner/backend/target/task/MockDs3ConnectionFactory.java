/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.PreviousVersions;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.platform.domain.JobToReplicate;
import com.spectralogic.s3.common.platform.spectrads3.BlobPersistenceContainer;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectsResult;
import com.spectralogic.s3.common.rpc.target.Ds3Connection;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.shutdown.ShutdownListener;

public final class MockDs3ConnectionFactory implements Ds3ConnectionFactory, Ds3Connection
{
    public Ds3Connection discover( final Ds3Target target )
    {
        return m_connection;
    }
    
    
    public MockDs3ConnectionFactory setConnectException( final RuntimeException ex )
    {
        m_connectException = ex;
        return this;
    }

    
    public Ds3Connection connect( final UUID userId, final Ds3Target target )
    {
        ++m_connectCallCount;
        final RuntimeException ex = m_connectException;
        if ( null != ex )
        {
            throw ex;
        }
        
        return m_connection;
    }
    
    
    @Override public void undeleteObject( final S3Object object, final String bucketName )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }
    
    
    public int getConnectCallCount()
    {
        return m_connectCallCount;
    }


    public boolean isShutdown()
    {
        return false;
    }


    public void shutdown()
    {
        // empty
    }


    public void verifyNotShutdown()
    {
        // empty
    }


    public void addShutdownListener( final ShutdownListener listener )
    {
        // empty
    }


    public List< ShutdownListener > getShutdownListeners()
    {
        return null;
    }


    public void shutdownOccurred()
    {
        // empty
    }


    public boolean isShutdownListenerNotificationCritical()
    {
        return false;
    }


    public void verifyIsAdministrator()
    {
        final RuntimeException ex = m_verifyIsAdministratorException;
        if ( null != ex )
        {
            throw ex;
        }
    }
    
    
    public MockDs3ConnectionFactory setVerifyIsAdministratorException( final RuntimeException ex )
    {
        m_verifyIsAdministratorException = ex;
        return this;
    }


    public Set< DataPolicy > getDataPolicies()
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }


    public Set< User > getUsers()
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }


    public void createUser( final User user, final String dataPolicy )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }


    public void updateUser( final User user )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }


    public void deleteUser( final String userName )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }


    public void createDs3Target( final Ds3Target target )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }
    
    
    public DeleteObjectsResult deleteObjects(
            final PreviousVersions previousVersions,
            final Bucket bucket,
            final Collection< S3Object > objects )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }
    
    
    public void deleteBucket( final String bucketName, final boolean deleteObjects )
    {
        throw new UnsupportedOperationException( "Looks like I need implementing ;)" );
    }


    public boolean isBucketExistant( final String bucketName )
    {
        final RuntimeException ex = m_isBucketExistantException;
        if ( null != ex )
        {
            throw ex;
        }
        
        return m_isBucketExistantResponse;
    }
    
    
    public MockDs3ConnectionFactory setIsBucketExistantResponse( final boolean response )
    {
        setIsBucketExistantException( null );
        m_isBucketExistantResponse = response;
        return this;
    }
    
    
    public MockDs3ConnectionFactory setIsBucketExistantException( final RuntimeException ex )
    {
        m_isBucketExistantException = ex;
        return this;
    }


    public boolean isJobExistant( final UUID jobId )
    {
        final RuntimeException ex = m_isJobExistantException;
        if ( null != ex )
        {
            throw ex;
        }
        
        return m_isJobExistantResponse;
    }
    
    
    public MockDs3ConnectionFactory setIsJobExistantResponse( final boolean response )
    {
        setIsJobExistantException( null );
        m_isJobExistantResponse = response;
        return this;
    }
    
    
    public MockDs3ConnectionFactory setIsJobExistantException( final RuntimeException ex )
    {
        m_isJobExistantException = ex;
        return this;
    }


    public boolean isChunkAllocated( final UUID jobId, final UUID chunkId )
    {
        final RuntimeException ex = m_isChunkAllocatedException;
        if ( null != ex )
        {
            throw ex;
        }
        
        return m_isChunkAllocatedResponse;
    }

    @Override
    public List<UUID> getBlobsReady(UUID jobId) {
        return m_readyBlobs;
    }


    public MockDs3ConnectionFactory setBlobsReady( final List<UUID> blobsReady ) {
        m_readyBlobs = blobsReady;
        return this;
    }


    public MockDs3ConnectionFactory setIsChunkAllocatedResponse( final boolean response )
    {
        setIsChunkAllocatedException( null );
        m_isChunkAllocatedResponse = response;
        return this;
    }
    
    
    public MockDs3ConnectionFactory setIsChunkAllocatedException( final RuntimeException ex )
    {
        m_isChunkAllocatedException = ex;
        return this;
    }
    
    
    public Boolean getChunkReadyToRead( final UUID chunkId )
    {
        final RuntimeException ex = m_getChunkReadyToReadException;
        if ( null != ex )
        {
            throw ex;
        }
        
        return m_getChunkReadyToReadResponse;
    }
    
    
    public MockDs3ConnectionFactory setGetChunkReadyToReadResponse( final Boolean response )
    {
        setGetChunkReadyToReadException( null );
        m_getChunkReadyToReadResponse = response;
        return this;
    }
    
    
    public MockDs3ConnectionFactory setGetChunkReadyToReadException( final RuntimeException ex )
    {
        m_getChunkReadyToReadException = ex;
        return this;
    }


    public MockDs3ConnectionFactory setGetChunkPendingTargetCommitResponse( final Boolean response )
    {
        setGetChunkPendingTargetCommitException( null );
        m_getChunkPendingTargetCommitResponse = response;
        return this;
    }
    
    
    public MockDs3ConnectionFactory setGetChunkPendingTargetCommitException( final RuntimeException ex )
    {
        m_getChunkPendingTargetCommitException = ex;
        return this;
    }


    public void createBucket( final UUID bucketId, final String bucketName, final String dataPolicy )
    {
        final RuntimeException ex = m_createBucketException;
        if ( null != ex )
        {
            throw ex;
        }
    }

    @Override
    public UUID createGetJob(Job sourceJob, Collection<JobEntry> entries, String bucketName) {
        final RuntimeException ex = m_createGetJobException;
        if ( null != ex )
        {
            throw ex;
        }

        return m_createGetJobResponse.getId();
    }


    public MockDs3ConnectionFactory setCreateBucketException( final RuntimeException ex )
    {
        m_createBucketException = ex;
        return this;
    }


    public MockDs3ConnectionFactory setCreateGetJobResponse( final JobToReplicate response )
    {
        setCreateGetJobException( null );
        m_createGetJobResponse = response;
        return this;
    }
    
    
    public MockDs3ConnectionFactory setCreateGetJobException( final RuntimeException ex )
    {
        m_createGetJobException = ex;
        return this;
    }
    
    
    public void verifySafeToCreatePutJob( final String bucketName )
    {
        final RuntimeException ex = m_verifySafeToCreatePutJobException;
        if ( null != ex )
        {
            throw ex;
        }
    }
    
    
    public MockDs3ConnectionFactory setVerifySafeToCreatePutJobException( final RuntimeException ex )
    {
        m_verifySafeToCreatePutJobException = ex;
        return this;
    }


    public void replicatePutJob( final DetailedJobToReplicate jobToReplicate, final String bucketName )
    {
        final RuntimeException ex = m_replicatePutJobException;
        if ( null != ex )
        {
            throw ex;
        }
    }
    
    
    public MockDs3ConnectionFactory setReplicatePutJobException( final RuntimeException ex )
    {
        m_replicatePutJobException = ex;
        return this;
    }


    public void cancelGetJob( final UUID sourceJobId )
    {
        final RuntimeException ex = m_cancelGetJobException;
        if ( null != ex )
        {
            throw ex;
        }
    }
    
    
    public MockDs3ConnectionFactory setCancelGetJobException( final RuntimeException ex )
    {
        m_cancelGetJobException = ex;
        return this;
    }


    public BlobPersistenceContainer getBlobPersistence( final UUID jobId, final Set< UUID > blobIds )
    {
        final RuntimeException ex = m_getBlobPersistenceException;
        if ( null != ex )
        {
            throw ex;
        }
        
        return m_getBlobPersistenceResponse;
    }
    
    
    public MockDs3ConnectionFactory setGetBlobPersistenceResponse( final BlobPersistenceContainer response )
    {
        setGetBlobPersistenceException( null );
        m_getBlobPersistenceResponse = response;
        return this;
    }
    
    
    public MockDs3ConnectionFactory setGetBlobPersistenceException( final RuntimeException ex )
    {
        m_getBlobPersistenceException = ex;
        return this;
    }
    
    
    public void getBlob(
            final UUID jobId,
            final String bucketName,
            final String objectName,
            final Blob blob,
            final File fileInCache )
    {
        final RuntimeException ex = m_getBlobException;
        if ( null != ex )
        {
            throw ex;
        }
    }
    
    
    public MockDs3ConnectionFactory setGetBlobException( final RuntimeException ex )
    {
        m_getBlobException = ex;
        return this;
    }


    public void putBlob(
            final UUID jobId,
            final String bucketName,
            final String objectName,
            final Blob blob,
            final File fileInCache,
            final Date objectCreationDate,
            final Set< S3ObjectProperty > metadata )
    {
        m_putBlobCalls.add( new PutBlobRequest( blob, objectName, metadata ) );
        
        final RuntimeException ex = m_putBlobException;
        if ( null != ex )
        {
            throw ex;
        }
    }
    
    
    public List< PutBlobRequest > getPutBlobCalls()
    {
        return new ArrayList<>( m_putBlobCalls );
    }
    
    
    public Map< String, Map< Long, Integer > > getPutBlobMap()
    {
        final Map< String, Map< Long, Integer > > retval = new HashMap<>();
        for ( final PutBlobRequest r : getPutBlobCalls() )
        {
            if ( !retval.containsKey( r.getObjectName() ) )
            {
                retval.put( r.getObjectName(), new HashMap< Long, Integer >() );
            }
            retval.get( r.getObjectName() ).put(
                    Long.valueOf( r.getOffset() ),
                    Integer.valueOf( r.getMetadata().size() ) );
        }
        
        return retval;
    }
    
    
    public MockDs3ConnectionFactory setPutBlobException( final RuntimeException ex )
    {
        m_putBlobException = ex;
        return this;
    }
    
    
    public void keepJobAlive( final UUID jobId )
    {
        final RuntimeException ex = m_keepJobAliveException;
        if ( null != ex )
        {
            throw ex;
        }
    }
    
    
    public MockDs3ConnectionFactory setKeepJobAliveException( final RuntimeException ex )
    {
        m_keepJobAliveException = ex;
        return this;
    }
    
    
    public BasicTestsInvocationHandler getBtih()
    {
        return m_btih;
    }
    
    
    public void setInvocationHandler( final InvocationHandler ih )
    {
        m_ih = ih;
    }
    
    
    public final static class PutBlobRequest
    {
        private PutBlobRequest(
                final Blob blob,
                final String objectName,
                final Set< S3ObjectProperty > metadata )
        {
            m_blob = blob;
            m_objectName = objectName;
            final Map< String, String > metadatas = new HashMap<>();
            for ( final S3ObjectProperty op : metadata )
            {
                metadatas.put( op.getKey(), op.getValue() );
            }
            m_metadata = metadatas;
        }
        
        
        public UUID getBlobId()
        {
            return m_blob.getId();
        }
        
        
        public long getOffset()
        {
            return m_blob.getByteOffset();
        }
        
        
        public String getObjectName()
        {
            return m_objectName;
        }
        
        
        public Map< String, String > getMetadata()
        {
            return new HashMap<>( m_metadata );
        }
        
        
        private final Blob m_blob;
        private final String m_objectName;
        private final Map< String, String > m_metadata;
    } // end inner class def
    

    private volatile int m_connectCallCount;
    private volatile List<UUID> m_readyBlobs = List.of();
    private volatile RuntimeException m_connectException;
    private volatile RuntimeException m_verifyIsAdministratorException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile boolean m_isBucketExistantResponse;
    private volatile RuntimeException m_isBucketExistantException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile boolean m_isJobExistantResponse;
    private volatile RuntimeException m_isJobExistantException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile boolean m_isChunkAllocatedResponse;
    private volatile RuntimeException m_isChunkAllocatedException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile Boolean m_getChunkReadyToReadResponse;
    private volatile RuntimeException m_getChunkReadyToReadException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile Boolean m_getChunkPendingTargetCommitResponse;
    private volatile RuntimeException m_getChunkPendingTargetCommitException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile RuntimeException m_createBucketException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile JobToReplicate m_createGetJobResponse;
    private volatile RuntimeException m_createGetJobException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile RuntimeException m_verifySafeToCreatePutJobException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile RuntimeException m_replicatePutJobException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile RuntimeException m_cancelGetJobException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile BlobPersistenceContainer m_getBlobPersistenceResponse;
    private volatile RuntimeException m_getBlobPersistenceException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile RuntimeException m_getBlobException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile RuntimeException m_putBlobException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile RuntimeException m_keepJobAliveException =
            new RuntimeException( "Did not expect a call to this method." );
    private volatile InvocationHandler m_ih;
    
    private final List< PutBlobRequest > m_putBlobCalls = new CopyOnWriteArrayList<>();
    
    private final BasicTestsInvocationHandler m_btih =
            new BasicTestsInvocationHandler( new InvocationHandler()
            {
                public Object invoke( final Object proxy, final Method method, final Object[] args )
                        throws Throwable
                {
                    if ( null != m_ih )
                    {
                        m_ih.invoke( proxy, method, args );
                    }
                    return method.invoke( MockDs3ConnectionFactory.this, args );
                }
            } );
    private final Ds3Connection m_connection =
            InterfaceProxyFactory.getProxy( Ds3Connection.class, m_btih );
}
