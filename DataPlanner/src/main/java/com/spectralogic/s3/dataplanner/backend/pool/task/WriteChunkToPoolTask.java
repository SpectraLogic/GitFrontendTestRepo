/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolFailureType;
import com.spectralogic.s3.common.dao.domain.shared.*;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.service.ds3.LocalBlobDestinationService;
import com.spectralogic.s3.common.dao.service.ds3.JobEntryService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.ds3.ObsoletionService;
import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.common.dao.service.pool.PoolFailureService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.dao.service.pool.PoolService.PoolAccessType;
import com.spectralogic.s3.common.dao.service.tape.BlobTapeService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.iom.IomUtils;
import com.spectralogic.s3.common.platform.iom.PersistenceProfile;
import com.spectralogic.s3.common.rpc.dataplanner.DiskFileInfo;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import com.spectralogic.s3.dataplanner.backend.frmwrk.LocalWriteDirective;
import com.spectralogic.s3.dataplanner.backend.frmwrk.ReadDirective;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.s3.dataplanner.backend.pool.frmwrk.PoolUtils;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.render.BytesRenderer;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;
import lombok.NonNull;

import static java.nio.file.StandardOpenOption.*;

public final class WriteChunkToPoolTask extends BasePoolTask
{
    public  WriteChunkToPoolTask(
            @NonNull final LocalWriteDirective writeDirective,
            @NonNull final BeansServiceManager serviceManager,
            final PoolEnvironmentResource poolEnvironmentResource,
            final PoolLockSupport<PoolTask> lockSupport,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager)
    {
        super( writeDirective.getPriority(), serviceManager, poolEnvironmentResource, lockSupport, diskManager, jobProgressManager );
        initWorkPool();
        m_writeDirective = writeDirective;
        m_bucketName = m_writeDirective.getBucket().getName();
        m_storageDomainId = m_writeDirective.getStorageDomain().getId();
    }

    
    @Override
    protected UUID selectPool()
    {
        m_serviceManager.getService(JobEntryService.class).verifyEntriesExist(BeanUtils.extractPropertyValues(m_writeDirective.getEntries(), Identifiable.ID));
        synchronized ( getLockSupport() )
        {
            final WriteChunkPoolSelectionStrategy poolSelectionStrategy =
                    new WriteChunkPoolSelectionStrategy( getServiceManager() );
            final UUID retval = poolSelectionStrategy.selectPool(
                    m_writeDirective.getSizeInBytes(),
                    m_writeDirective.getStorageDomain().getId(),
                    m_writeDirective.getBucket().getId(),
                    getLockSupport().getPoolsUnavailableForWriteLock(m_writeDirective.getSizeInBytes()) );
            if ( null == retval )
            {
                return null;
            }
            
            final Pool pool = getServiceManager().getRetriever( Pool.class ).attain( retval );
            getLockSupport().acquireWriteLock( retval, this, m_writeDirective.getSizeInBytes(), pool.getAvailableCapacity() );
            return retval;
        }
    }

    // Used to run zpool sync and sync calls
    private boolean runExec(final String cmd, final boolean withTimeout)
    {
        final int timeout = 5;
        final TimeUnit timeUnit = TimeUnit.MINUTES;
        try
        {
            final Runtime runtime = Runtime.getRuntime();
            final Process process = runtime.exec( cmd );
            if ( withTimeout && !process.waitFor(timeout, timeUnit) )
            {
                process.destroy();
                LOG.error( "Timed out: " + cmd );
                return false;
            }
            return true;
        }
        catch ( final IOException|InterruptedException e )
        {
            LOG.error( "Failed to run: " + cmd, e );
            return false;
        }
    }

    
    @Override
    protected BlobStoreTaskState runInternal()
    {
        if ( m_writeDirective.getEntries().isEmpty() )
        {
            LOG.warn( "No job entries found. Marking task as completed." );
            return BlobStoreTaskState.COMPLETED;
        }
        final Map< UUID, Blob > blobs = BeanUtils.toMap(
                getServiceManager().getRetriever( Blob.class ).retrieveAll( 
                        BeanUtils.extractPropertyValues(
                                m_writeDirective.getEntries(), BlobObservable.BLOB_ID ) )
                        .toSet() );
        final Map< UUID, S3Object > objects = BeanUtils.toMap( 
                getServiceManager().getRetriever( S3Object.class ).retrieveAll( 
                        BeanUtils.extractPropertyValues( blobs.values(), Blob.OBJECT_ID ) )
                        .toSet() );
        
        final BytesRenderer bytesRenderer = new BytesRenderer();
        final String dataDescription =
                m_writeDirective.getEntries() + " blobs (" + bytesRenderer.render( m_writeDirective.getSizeInBytes() ) + ")";
        LOG.info( "Will write " + dataDescription + "..." );

        getServiceManager().getService( PoolService.class ).updateDates( 
                getPoolId(), PoolAccessType.MODIFIED );
        try
        {
            final Duration duration = new Duration();
            final Pool pool = getPool();
            final Set< BlobPool > blobPoolsToCreate = new HashSet<>();
            final Set< BlobPool > blobPoolsToUpdate = new HashSet<>();
            final Set< Future< ? > > futures = new HashSet<>();
            for ( final JobEntry jobEntry : m_writeDirective.getEntries() )
            {
                final Blob blob = blobs.get( jobEntry.getBlobId() );
                //NOTE: no need to check for existing blob pools if this is not a stage job
                BlobPool existingBlobPoolToUpdate = m_writeDirective.isStageJob() ? getExistingBlobPool(blob) : null;
                if (existingBlobPoolToUpdate != null) {
                    //This is a stage job and we already have the file on pool, we just need to update it.
                    blobPoolsToUpdate.add(existingBlobPoolToUpdate);
                } else {
                    final S3Object object = objects.get( blob.getObjectId() );

                    setupObjectDirectory( pool, object );
                    futures.add( s_writeWP.submit(
                            new BlobWriter( getDiskManager(), pool, m_bucketName, object, blob, getServiceManager().getService(BlobPoolService.class) ) ) );

                    blobPoolsToCreate.add( BeanFactory.newBean( BlobPool.class )
                            .setBlobId( blob.getId() )
                            .setBucketId( object.getBucketId() )
                            .setPoolId( pool.getId() ) );
                    if ( 1000 == futures.size() )
                    {
                        quiesceOnFutures( futures );
                    }
                }
            }

            quiesceOnFutures( futures );
            LOG.info( dataDescription + " written to pool at " 
                      + bytesRenderer.render( m_writeDirective.getSizeInBytes(), duration ) + "." );

            // Sync the pool after the chunk has written but before DB entries updated
            final boolean poolSafetyEnabled =
                    getServiceManager().getRetriever( DataPathBackend.class ).attain( Require.nothing() ).getPoolSafetyEnabled();
            if ( poolSafetyEnabled )
            {
                final String zPoolSyncCmd = "zpool sync " + this.getPool().getName();
                final boolean success = runExec(zPoolSyncCmd, true);
                if ( !success )
                {
                    runExec("sync", false);
                }
            }
            if ( m_writeDirective.isVerifyAfterWrite() )
            {
                final ReadDirective readDirectiveForVerify =
                        new ReadDirective( m_writeDirective.getPriority(),
                                getPool().getId(),
                                PersistenceType.POOL,
                                m_writeDirective.getEntries().stream().toList());
                final VerifyChunkOnPoolTask verifyTask = new VerifyChunkOnPoolTask(
                        readDirectiveForVerify,
                        m_serviceManager,
                        true ,
                        true,
                        getPoolEnvironmentResource(),
                        getLockSupport(),
                        getDiskManager(),
                        getJobProgressManager() );
                LOG.info( "Verify after write enabled.  Will verify chunk written using " 
                          + verifyTask + "." );
                verifyTask.runAsNestedTaskInsideAnotherTask( getPoolId() );
                if ( BlobStoreTaskState.COMPLETED != verifyTask.getState() )
                {
                    throw new RuntimeException( "Verify task failed to complete." );
                }
            }

            Obsoletion obsoletion = null;
            try (final NestableTransaction transaction = getServiceManager().startNestableTransaction())
            {
                final Set< UUID > blobIds = BeanUtils.extractPropertyValues( blobPoolsToCreate, BlobObservable.BLOB_ID);
                final Set< BlobPool > blobPoolsToObsolete = transaction.getRetriever( BlobPool.class ).retrieveAll(
                        Require.all(
                                IomUtils.blobPoolPersistedToStorageDomain(
                                        m_storageDomainId,
                                        //NOTE: do not re-obsolete blob records that are already obsolete
                                        PersistenceProfile.EVERYTHING_BUT_OBSOLETE ),
                                Require.beanPropertyEqualsOneOf(
                                        BlobObservable.BLOB_ID,
                                        blobIds ) ) ).toSet();
                final Set< BlobTape > blobTapesToObsolete = transaction.getRetriever( BlobTape.class ).retrieveAll(
                        Require.all(
                                IomUtils.blobTapePersistedToStorageDomain(
                                		m_storageDomainId,
                                        //NOTE: do not re-obsolete blob records that are already obsolete
                                        PersistenceProfile.EVERYTHING_BUT_OBSOLETE ),
                                Require.beanPropertyEqualsOneOf(
                                        BlobObservable.BLOB_ID,
                                        blobIds ) ) ).toSet(); 
                if ( !blobPoolsToObsolete.isEmpty() )
                {
                    LOG.warn( blobPoolsToObsolete.size() + " blob pool records are now obsolete and will be kept track"
                            + " of to be deleted following the next database backup.");
                    obsoletion = BeanFactory.newBean( Obsoletion.class );
                    transaction.getService( ObsoletionService.class ).create( obsoletion );
                    transaction.getService( BlobPoolService.class ).obsoleteBlobPools(
                        blobPoolsToObsolete,
                        obsoletion.getId() );
                }
                if ( !blobTapesToObsolete.isEmpty() )
                {
                    LOG.warn( blobTapesToObsolete.size() + " blob tape records are now obsolete and will be kept track"
                            + " of to be deleted following the next database backup.");
                    obsoletion = BeanFactory.newBean( Obsoletion.class );
                    transaction.getService( ObsoletionService.class ).create( obsoletion );
                    transaction.getService( BlobTapeService.class ).obsoleteBlobTapes(
                        blobTapesToObsolete,
                        obsoletion.getId() );
                }
                transaction.getService( BlobPoolService.class ).create( blobPoolsToCreate );

                if (!blobPoolsToUpdate.isEmpty()) {
                    LOG.info("Will update the dates of " + blobPoolsToUpdate.size() + " blob pool records for stage job(s)");
                    final Date currentDate = new Date();
                    transaction.getUpdater(BlobPool.class).update(
                            Require.beanPropertyEqualsOneOf(Identifiable.ID, BeanUtils.toMap(blobPoolsToUpdate).keySet()),
                            b -> b.setDateWritten(currentDate).setLastAccessed(currentDate),
                            BlobPool.DATE_WRITTEN,
                            BlobPool.LAST_ACCESSED);
                }
                final LocalBlobDestinationService service =
                        transaction.getService( LocalBlobDestinationService.class );

                final Set<UUID> destinationIds = BeanUtils.extractPropertyValues(m_writeDirective.getDestinations(), Identifiable.ID);
                service.update(
                        Require.beanPropertyEqualsOneOf(Identifiable.ID, destinationIds),
                        (pt) -> pt.setBlobStoreState(JobChunkBlobStoreState.COMPLETED),
                        LocalBlobDestination.BLOB_STORE_STATE);

                transaction.commitTransaction();
            }
            if ( null != obsoletion )
            {
                //NOTE: we create the obsoletion after the transaction is closed to ensure that it has a date that is
                //later than the records were committed.
                m_serviceManager.getService( ObsoletionService.class )
                        .update( obsoletion.setDate( new Date() ), Obsoletion.DATE );
            }
            return BlobStoreTaskState.COMPLETED;
        }
        catch ( final RuntimeException ex )
        {
            handleFailure( ex );
            throw ExceptionUtil.toRuntimeException( ex );
        }
        finally
        {
            getServiceManager().getService( PoolService.class ).updateDates( 
                    getPoolId(), PoolAccessType.MODIFIED );
        }
    }

    private BlobPool getExistingBlobPool(Blob blob) {
        //NOTE: the existing blob pool could be anywhere in the storage domain, not necessarily on this specific pool
        return getServiceManager().getRetriever(BlobPool.class).retrieve(
                Require.all(
                        IomUtils.blobPoolPersistedToStorageDomain(
                                m_storageDomainId,
                                PersistenceProfile.PERFECT),
                        Require.beanPropertyEquals(
                                BlobObservable.BLOB_ID,
                                blob.getId())));
    }


    private void quiesceOnFutures( final Set< Future< ? > > futures )
    {
        for ( final Future< ? > f : futures )
        {
            try
            {
                f.get();
            }
            catch ( final Exception ex )
            {
                throw ExceptionUtil.toRuntimeException( ex );
            }
        }
        
        futures.clear();
    }
    
    
    private void setupObjectDirectory( final Pool pool, final S3Object object )
    {
        final Path dirObject = PoolUtils.getPath( pool, m_bucketName, object.getId(), null );
        final Path fileObjectProps = PoolUtils.getPropsFile( dirObject );
        
        PoolUtils.verifyObjectDir( dirObject );
        try
        {
            Files.createDirectories( dirObject );
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Unable to create object directory " + dirObject.getParent(), ex );
        }
        
        // Object Metadata
        final Path fileObjectMetadata = PoolUtils.getMetadataFile( dirObject );
        final Path fileObjectMetadataPart = PoolUtils.getMetadataPartFile( dirObject );
        if ( !Files.exists( fileObjectMetadata ) )
        {
            final Set< S3ObjectProperty > objectMetadata = getServiceManager().getRetriever( S3ObjectProperty.class )
                                                                              .retrieveAll( Require.beanPropertyEquals(
                                                                                      S3ObjectProperty.OBJECT_ID,
                                                                                      object.getId() ) )
                                                                              .toSet();
            final Properties objectMetadataProps = new Properties();
            if ( null != object.getCreationDate() )
            {
                objectMetadataProps.put( KeyValueObservable.CREATION_DATE, String.valueOf( object.getCreationDate()
                                                                                                 .getTime() ) );
            }
            objectMetadataProps.put( KeyValueObservable.TOTAL_BLOB_COUNT, String.valueOf(
                    getServiceManager().getRetriever( Blob.class )
                                       .getCount( Blob.OBJECT_ID, object.getId() ) ) );
            for ( S3ObjectProperty objProp : objectMetadata )
            {
                objectMetadataProps.setProperty( objProp.getKey(), objProp.getValue() );
            }
            try
            {
                final OutputStream out = Files.newOutputStream( fileObjectMetadataPart );
                objectMetadataProps.store( out, "---No Comment---" );
                out.close();
            }
            catch ( IOException ex1 )
            {
                throw new RuntimeException( "Failed to write metadata file: " + fileObjectMetadataPart.toString(),
                        ex1 );
            }
            PoolUtils.renameFile( fileObjectMetadataPart, fileObjectMetadata );
        }
        
        // Object Properties
        final Path fileObjectPropsPart = PoolUtils.getPropsPartFile( dirObject );
        if ( !Files.exists( fileObjectProps ) )
        {
            final Properties objectNameProps = new Properties();
            objectNameProps.setProperty( NameObservable.NAME, object.getName() );
            try
            {
                final OutputStream out = Files.newOutputStream( fileObjectPropsPart );
                objectNameProps.store( out, "---No Comment---" );
                out.close();
            }
            catch ( IOException ex1 )
            {
                throw new RuntimeException( "Failed to write props file: " + fileObjectPropsPart, ex1 );
            }
            PoolUtils.renameFile( fileObjectPropsPart, fileObjectProps );
        }
    }
    
    
    private final static class BlobWriter implements Runnable
    {
        private BlobWriter(
                final DiskManager diskManager,
                final Pool pool,
                final String bucketName,
                final S3Object object,
                final Blob blob,
                final BlobPoolService blobPoolService)
        {
            m_diskManager = diskManager;
            m_pool = pool;
            m_bucketName = bucketName;
            m_object = object;
            m_blob = blob;
            m_blobPoolService = blobPoolService;
        }
        
        
        @Override
        public void run()
        {
            final Path fileBlob = PoolUtils.getPath( m_pool, m_bucketName, m_object.getId(), m_blob.getId() );
            if ( Files.exists( fileBlob ) )
            {
                LOG.info( "File for blob " + m_blob.getId() + " already exists (will delete it)." );
                boolean deleted = false;
                try
                {
                    Files.delete( fileBlob );
                    deleted = true;
                }
                catch ( IOException ignored )
                {
                }
                if ( !deleted )
                {
                    throw new RuntimeException(
                            "Failed to delete existing file as a prerequisite step to write blob "
                            + m_blob.getId() + ": " + fileBlob );
                }
            }
    
            FileChannel inChannel = null;
            FileChannel outChannel = null;
            final DiskFileInfo diskFileInfo = m_diskManager.getDiskFileFor( m_blob.getId() );
            final Path inPath = Paths.get( diskFileInfo.getFilePath() );
            final Path outPath = Paths.get( fileBlob.toString() );
            try
            {
                try {
                    inChannel = FileChannel.open( inPath );
                } catch (final Exception e) {
                    m_blobPoolService.registerFailureToRead(diskFileInfo);
                    throw e;
                }
                
                final Set< OpenOption > options = new HashSet<>();
                options.add( WRITE );
                options.add( CREATE );

                outChannel = FileChannel.open( outPath, options );

                long bytesTransferred = 0;
                while ( bytesTransferred < inChannel.size() )
                {
                    bytesTransferred += inChannel.transferTo( bytesTransferred, inChannel.size(), outChannel );
                }
                // The force is a no-op if all the data has already landed on disk via the SYNC option on open
                outChannel.force( false );
            }
            catch ( Exception ex )
            {
                throw new RuntimeException( "Failed to write blob " + m_blob.getId() + " to " + fileBlob + ".", ex );
            }
            finally
            {
                try
                {
                    if ( inChannel != null )
                    {
                        inChannel.close();
                    }
                }
                catch ( IOException ex )
                {
                    LOG.warn( "Failed closing read channel for " + inPath, ex );
                }
                try
                {
                    if ( outChannel != null )
                    {
                        outChannel.close();
                    }
                }
                catch ( IOException ex )
                {
                    LOG.warn( "Failed closing write channel for " + outPath, ex );
                }
            }
            
            final Path fileBlobProps = PoolUtils.getPropsFile( fileBlob );
            final Path fileBlobPropsPart = PoolUtils.getPropsPartFile( fileBlob );
            final Properties blobProps = new Properties();
            blobProps.setProperty( Blob.BYTE_OFFSET, String.valueOf( m_blob.getByteOffset() ) );
            blobProps.setProperty( ChecksumObservable.CHECKSUM_TYPE, m_blob.getChecksumType().toString() );
            blobProps.setProperty( ChecksumObservable.CHECKSUM, m_blob.getChecksum() );
    
            try
            {
                final OutputStream out = Files.newOutputStream( fileBlobPropsPart );
                blobProps.store( out, "---No Comment---" );
                out.close();
            }
            catch ( final IOException ex )
            {
                throw new RuntimeException( "Failed to write blob properties file: " + fileBlobPropsPart, ex );
            }
    
            PoolUtils.renameFile( fileBlobPropsPart, fileBlobProps );
        }
        
        
        private final DiskManager m_diskManager;
        private final Pool m_pool;
        private final String m_bucketName;
        private final S3Object m_object;
        private final Blob m_blob;
        private final BlobPoolService m_blobPoolService;
    } // end inner class def
    
    
    private void handleFailure( final Exception ex )
    {
        getServiceManager().getService( PoolFailureService.class ).create(
                getPoolId(), PoolFailureType.WRITE_FAILED, ex );
    }

    
    @Override
    public String getDescription() {
        return "Write " + m_writeDirective.getEntries().size() + " blobs to pool.";
    }

    @Override
    public UUID[] getJobIds() {
        return BeanUtils.extractPropertyValues(m_writeDirective.getEntries(), JobEntry.JOB_ID).toArray(new UUID[0]);
    }


    /**
     * This init approach is used to work most easily in conjunction with test
     * run thread lead detection and prevention. Please don't change it unless
     * you're explicitly working on test run thread leak detection/prevention.
     */
    private static void initWorkPool()
    {
        if( null == s_writeWP || s_writeWP.isShutdown() )
        {
            s_writeWP = WorkPoolFactory.createBoundedWorkPool(
                    NUM_WRITE_THREADS,
                    NUM_WRITE_THREADS,
                    WriteChunkToPoolTask.class.getSimpleName() );
        }
    }


    private final String m_bucketName;
    private final UUID m_storageDomainId;
    /* cached sizes; if this is zero it is not valid */
    private final LocalWriteDirective m_writeDirective;
    private final static int NUM_WRITE_THREADS = 8;
    private static WorkPool s_writeWP = null;
}
