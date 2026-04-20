/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.LtfsFileNamingMode;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.shared.ImportPersistenceTargetDirective;
import com.spectralogic.s3.common.dao.domain.shared.ImportTapeTargetDirective;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.orm.TapeRM;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainService;
import com.spectralogic.s3.common.dao.service.shared.ImportDirectiveService;
import com.spectralogic.s3.common.dao.service.tape.TapeFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.importer.BaseImportHandler;
import com.spectralogic.s3.dataplanner.backend.importer.PersistenceTargetImportHandler;
import com.spectralogic.s3.dataplanner.backend.importer.PersistenceTargetImporter;
import com.spectralogic.s3.dataplanner.backend.tape.api.LongRunningInterruptableTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.api.StaticTapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public abstract class BaseImportTapeTask
    < ID extends ImportTapeTargetDirective< ID > & DatabasePersistable > 
    extends BaseBlobTask
    implements LongRunningInterruptableTapeTask, StaticTapeTask
{
    protected BaseImportTapeTask(
            final Class<? extends ImportDirectiveService<ID>> importDirectiveService,
            final TapeState importableState,
            final TapeState importPendingState,
            final TapeState importInProgressState,
            final BlobStoreTaskPriority priority,
            final UUID tapeId,
            final BlobStore blobStore,
            final boolean canWriteNewDataToTapeAfterImport,
            final DiskManager diskManager,
            final TapeFailureManagement tapeFailureManagement,
            final BeansServiceManager serviceManager)
    {
        super( priority, tapeId, diskManager, tapeFailureManagement, serviceManager );
        m_importDirectiveService = importDirectiveService;
        m_importableState = importableState;
        m_importPendingState = importPendingState;
        m_importInProgressState = importInProgressState;
        m_blobStore = blobStore;
        m_canWriteNewDataToTapeAfterImport = canWriteNewDataToTapeAfterImport;
    }
    
    
    @Override
    final protected void handlePreparedForExecution()
    {
        final Tape tape = getTape();
        if ( m_importPendingState != tape.getState() )
        {
            invalidateTaskAndThrow( "Tape is in state " + tape.getState() + "." );
        }
        
        getServiceManager().getService( TapeService.class ).transistState(
                tape, m_importInProgressState );
    }
    
    
    @Override
    final protected void handleExecutionFailed()
    {
        getServiceManager().getService( TapeService.class ).transistState(
                getTape(), m_importPendingState );
    }
    
    
    @Override
    protected void performPreRunValidations()
    {
        verifyTapeInDrive( new DefaultTapeInDriveVerifier(this, true ) );
    }

    
    @Override
    final protected BlobStoreTaskState runInternal()
    {
        final Tape tape = getTape();
        final WhereClause candidateStorageDomainFilterForPersistenceTarget = Require.all( 
                Require.beanPropertyEquals( 
                        StorageDomainMember.TAPE_PARTITION_ID, 
                        tape.getPartitionId() ),
                Require.beanPropertyEquals( 
                        StorageDomainMember.TAPE_TYPE,
                        tape.getType() ) );
        final ImportTapeHandler importHandler = new ImportTapeHandler();
        final PersistenceTargetImporter< BlobTape, Tape, ID, TapeFailureType > importer =
                new PersistenceTargetImporter<>(
                        BlobTape.class,
                        Tape.class,
                        getTapeId(), 
                        candidateStorageDomainFilterForPersistenceTarget, 
                        m_importDirectiveService,
                        ImportTapeTargetDirective.TAPE_ID, 
                        TapeFailureType.IMPORT_FAILED,
                        TapeFailureType.IMPORT_INCOMPLETE,
                        importHandler, 
                        getServiceManager(),
                        m_blobStore );
        try
        {
            if ( null != tape.getStorageDomainMemberId() )
            {
                final ImportDirectiveService< ID > itdService =
                        getServiceManager().getService( m_importDirectiveService );
                final ID importDirective =
                        itdService.attain( ImportTapeTargetDirective.TAPE_ID, getTapeId() );
                final UUID storageDomainId = new TapeRM( tape, getServiceManager() )
                        .getStorageDomainMember().getStorageDomain().getId();
                if ( null != importDirective.getStorageDomainId() 
                        && !importDirective.getStorageDomainId().equals( storageDomainId ) )
                {
                    throw new RuntimeException( 
                            "Cannot re-import tape into storage domain " 
                            + importDirective.getStorageDomainId() 
                            + " since it was previously assigned to storage domain " 
                            + storageDomainId + "." );
                }
                itdService.update( 
                        importDirective.setStorageDomainId( storageDomainId ),
                        ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID );
            }
            
            final BlobStoreTaskState result = importer.run();
            m_tapeFailureManagement.resetFailures(
                    getTapeId(),
                    getDriveId(),
                    TapeFailureType.IMPORT_FAILED,
                    TapeFailureType.IMPORT_FAILED_DUE_TO_DATA_INTEGRITY,
                    TapeFailureType.IMPORT_FAILED_DUE_TO_TAKE_OWNERSHIP_FAILURE);
            return result;
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "Failed to import tape " + getTapeId() + ".", ex );
            return importHandler.failed( TapeFailureType.IMPORT_FAILED, ex );
        }
    }
    
    
    private final class ImportTapeHandler 
        extends BaseImportHandler< TapeFailureType >
        implements PersistenceTargetImportHandler< TapeFailureType >
    {
        @Override
        public void openForRead()
        {
            updateTapeExtendedInformation();
            m_readHandle = openTapeForRead();
        }
        
        
        @Override
        public S3ObjectsOnMedia read()
        {
            final long preferredMaximumTotalBlobLengthInBytesReturned = getPageSegmentLength();
            return getDriveResource().readContents( 
                    m_readHandle, 
                    10000,
                    preferredMaximumTotalBlobLengthInBytesReturned ).get( Timeout.VERY_LONG );
        }

        
        @Override
        public TapeFailureType verify(
                final ImportPersistenceTargetDirective< ? > importDirective,
                final S3ObjectsOnMedia objects )
        {
            performCustomPopulation( objects );
            if ( !importDirective.isVerifyDataPriorToImport() )
            {
                return null;
            }
            
            return verifyOnTape( objects );
        }
    
    
        @Override public void verifyCompatibleStorageDomain( final UUID storageDomainMemberId )
        {
            verifyStorageDomainLtfsFileNamingMatchesTape( storageDomainMemberId );
        }
    
    
        @Override
        public void closeRead()
        {
            if ( null != m_readHandle )
            {
                getDriveResource().closeContents( m_readHandle ).get( Timeout.LONG );
            }
        }
        
        
        @Override
        public BlobStoreTaskState finalizeImport( final UUID storageDomainId, final UUID isolatedBucketId )
        {
            verifyStorageDomainLtfsFileNamingMatchesTape( storageDomainId );
            
            final boolean writeProtected = getTape().isWriteProtected();
            final boolean ltfsForeign = 
                    getTape().getState() == TapeState.LTFS_WITH_FOREIGN_DATA ||
                    getTape().getState() == TapeState.RAW_IMPORT_IN_PROGRESS ||
                    getTape().getState() == TapeState.RAW_IMPORT_PENDING;
            final boolean takeOwnershipPending = writeProtected && !ltfsForeign;
            final String checkpoint;
            try
            {
            	final TapeDriveType driveType =
                		new TapeRM( getTape(), m_serviceManager ).getPartition().getDriveType();
                if ( ltfsForeign )
                {
                    LOG.info( "Tape is LTFS with foreign data.  Cannot take ownership of it." );
                    checkpoint = getDriveResource().verifyConsistent().get( Timeout.VERY_LONG );
                }
                else if ( writeProtected )
                {
                    LOG.info( "Tape is write protected.  Cannot take ownership of it at this time." );
                    checkpoint = getDriveResource().verifyConsistent().get( Timeout.VERY_LONG );
                }
                else if ( !driveType.isWriteSupported( getTape().getType() ))
                {
                	LOG.info( "Cannot take ownership since tape " + getTape().getId() + " ("  + getTape().getBarCode()
                			+ ") is of type " + getTape().getType() + " and is not writeable in"
                			+ " a partition with drives of type " + driveType + ".");
                	checkpoint = getDriveResource().verifyConsistent().get( Timeout.VERY_LONG );
                }
                else
                {
                    checkpoint = 
                            getDriveResource().takeOwnershipOfTape( getTapeId() ).get( Timeout.VERY_LONG );
                }
            }
            catch ( final RuntimeException ex )
            {
                LOG.warn( "Failed to import tape " + getTapeId() + " due to take ownership failure.", ex );
                return failed( TapeFailureType.IMPORT_FAILED_DUE_TO_TAKE_OWNERSHIP_FAILURE, ex );
            }
            
            final BeansServiceManager transaction = getServiceManager().startTransaction();
            try
            {
            	final UUID storageDomainMemberId = transaction.getService( StorageDomainService.class )
                        .selectAppropriateStorageDomainMember( getTape(), storageDomainId );
                transaction.getService( TapeService.class ).update( 
                        getTape().setLastCheckpoint( checkpoint ).setStorageDomainMemberId( storageDomainMemberId )
                        .setAssignedToStorageDomain( true )
                        .setBucketId( isolatedBucketId ).setTakeOwnershipPending( takeOwnershipPending )
                        .setFullOfData( !m_canWriteNewDataToTapeAfterImport ),
                        Tape.LAST_CHECKPOINT, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                        PersistenceTarget.BUCKET_ID, PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                        Tape.TAKE_OWNERSHIP_PENDING,
                        Tape.FULL_OF_DATA );
                transaction.getService( TapeService.class ).transistState(
                        getTape(), TapeState.NORMAL );
                transaction.getService( TapeFailureService.class ).deleteAll( 
                        getTapeId(), TapeFailureType.IMPORT_FAILED );
                transaction.getService( TapeFailureService.class ).deleteAll( 
                        getTapeId(), TapeFailureType.REIMPORT_REQUIRED );
                transaction.commitTransaction();
                updateTapeExtendedInformation();
            }
            finally
            {
                transaction.closeTransaction();
            }
            return BlobStoreTaskState.COMPLETED;
        }
    
        
        @Override
        public BlobStoreTaskState failedInternal(
                final TapeFailureType failureType,
                final RuntimeException ex )
        {
            m_tapeFailureManagement.registerFailure(
                    getTapeId(),
                    failureType,
                    ex );
            
            final BeansServiceManager transaction = getServiceManager().startTransaction();
            try
            {
                transaction.getService( TapeService.class ).transistState(
                        getTape(), m_importableState );
                transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }
            return BlobStoreTaskState.COMPLETED;
        }
        
        
        @Override
        public void warnInternal(
                final TapeFailureType failureType,
                final RuntimeException ex )
        {
            m_tapeFailureManagement.registerFailure(
                    getTapeId(),
                    failureType,
                    ex );
        }
        
        
        private volatile String m_readHandle;
    } // end inner class def
    
    
    protected void performCustomPopulation( @SuppressWarnings( "unused" ) final S3ObjectsOnMedia ooms )
    {
        // by default, do nothing
    }
    
    
    protected abstract String openTapeForRead();
    
    
    protected abstract TapeFailureType verifyOnTape( final S3ObjectsOnMedia objects );
    
    
    protected void verifyStorageDomainLtfsFileNamingMatchesTape( final UUID storageDomainId )
    {
        final StorageDomain storageDomain = 
                m_serviceManager.getRetriever( StorageDomain.class ).attain( storageDomainId );
        if ( null == m_tapeLtfsFileNaming )
        {
            m_tapeLtfsFileNaming = getDriveResource().getLtfsFileNamingMode()
                                                     .get( Timeout.LONG );
        }
        if ( storageDomain.getLtfsFileNaming() != m_tapeLtfsFileNaming )
        {
            throw new RuntimeException(
                    "Cannot import tape into storage domain " + storageDomain.getId() 
                    + " (" + storageDomain.getName() + ") since the tape has a " +
                            LtfsFileNamingMode.class.getSimpleName() + " of " + m_tapeLtfsFileNaming
                    + ", but the storage domain is " + storageDomain.getLtfsFileNaming() + "." );
        }
    }
    
    
    @Override
    final public void dequeued()
    {
        final TapeService service = getServiceManager().getService( TapeService.class );
        final Tape tape = service.retrieve( m_defaultTapeId );
        if ( null != tape && m_importPendingState == tape.getState() )
        {
            getServiceManager().getService( TapeService.class ).rollbackLastStateTransition( tape );
        }
    }


    @Override
    final public String getDescription()
    {
        return "Import Tape " + m_defaultTapeId;
    }

    @Override
    public boolean allowMultiplePerTape() {
        return false;
    }
    
    
    private final BlobStore m_blobStore;
    private final boolean m_canWriteNewDataToTapeAfterImport;
    private final TapeState m_importableState;
    private final TapeState m_importPendingState;
    private final TapeState m_importInProgressState;
    private final Class< ? extends ImportDirectiveService< ID > > m_importDirectiveService;
    private LtfsFileNamingMode m_tapeLtfsFileNaming;
}
