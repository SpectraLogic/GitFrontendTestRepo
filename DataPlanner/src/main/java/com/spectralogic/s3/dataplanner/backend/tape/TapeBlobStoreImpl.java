/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.tape;

import java.util.*;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.orm.*;
import com.spectralogic.s3.common.dao.service.tape.*;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeEnvironmentImpl;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeEnvironmentManagerImpl;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.Refresh;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment;
import com.spectralogic.s3.dataplanner.backend.tape.task.*;
import lombok.NonNull;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.shared.ImportTapeTargetDirective;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTask;
import com.spectralogic.s3.common.rpc.tape.TapeEnvironmentResource;
import com.spectralogic.s3.common.rpc.tape.TapePartitionResource;
import com.spectralogic.s3.dataplanner.backend.frmwrk.VerifyMediaProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeBlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeLockSupport;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTask;
import com.spectralogic.s3.dataplanner.backend.tape.processor.EjectTapeDismountProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.EjectTapeProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.ForceRemovalTapeProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.OnlineTapeProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.ReclaimTapeProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.VerifyTapeProcessor;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeBlobStoreProcessorImpl;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.TapeBlobStoreProcessor;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.StaticCache;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.ForceFlagRequiredException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.client.RpcClient;
import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.shutdown.BaseShutdownable;

public final class TapeBlobStoreImpl extends BaseShutdownable implements TapeBlobStore
{
    public TapeBlobStoreImpl( 
            final RpcClient rpcClient,
            final DiskManager diskManager,
            final JobProgressManager jobProgressManager,
            final BeansServiceManager serviceManager )
    {
        Validations.verifyNotNull( "Job Progress Manager", jobProgressManager);
        Validations.verifyNotNull( "Disk Manager", diskManager);
        Validations.verifyNotNull( "Service Manager", serviceManager);

        m_serviceManager = serviceManager;
        m_jobProgressManager = jobProgressManager;
        m_diskManager = diskManager;
        
        LOG.info( getClass().getSimpleName() + " is starting up..." );
        final TapeLockSupport< Object > tapeLockSupport = new TapeLockSupportImpl<>(
                rpcClient,
                m_serviceManager );
        m_tapeFailureManagement = new TapeFailureManagement(
                serviceManager.getService(TapeFailureService.class),
                serviceManager.getService(TapeDriveService.class),
                serviceManager.getService(TapeService.class)
        );
        m_tapeEnvironment = new TapeEnvironmentImpl(
                new TapeEnvironmentManagerImpl(serviceManager, 2),
                createTapeEnvironmentResource( rpcClient ),
                m_serviceManager,
                tapeLockSupport,
                new StaticCache<>(new TapePartitionResourceProvider(rpcClient)),
                m_tapeFailureManagement );

        m_processor = new TapeBlobStoreProcessorImpl(
                tapeLockSupport, 
                serviceManager,
                m_tapeEnvironment,
                m_tapeFailureManagement,
                m_diskManager,
                m_jobProgressManager
        );
        m_tapeReclaimer = new ReclaimTapeProcessor(
                m_serviceManager,
                this, 
                m_processor.getTaskStateLock(),
                tapeLockSupport, 
                15 * 60 * 1000 );
        m_tapeEjector = new EjectTapeProcessor( 
                m_serviceManager, 
                m_processor,
                m_tapeEnvironment,
                60 * 1000 );
        m_tapeEjectorDismount = new EjectTapeDismountProcessor( m_serviceManager, m_processor, m_tapeEnvironment, 60 * 1000, m_tapeEjector );
        m_tapeForceRemover = new ForceRemovalTapeProcessor(
                m_serviceManager,
                m_tapeEnvironment,
                15000 );
        m_tapeOnliner = new OnlineTapeProcessor(
                m_serviceManager, 
                m_processor,
                m_tapeEnvironment,
                15000 );
        m_tapeVerificationDriver = new VerifyMediaProcessor<>(
                Tape.class,
                Require.not( Require.beanPropertyEqualsOneOf( 
                        Tape.STATE, TapeState.getStatesThatDisallowTapeLoadIntoDrive() ) ),
                BlobTape.class,
                VerifyTapeTask.class,
                m_serviceManager,
                this,
                15 * 60000,
                64 );
        m_tapeVerifier = new VerifyTapeProcessor(
                m_processor,
                m_serviceManager,
                m_diskManager,
                m_tapeFailureManagement,
                2 * 60000 );
        
        startPreviouslyRunningTasks();
        m_processor.setTapeEjector(this);
        m_processor.start();
        
        addShutdownListener( m_processor );
        addShutdownListener( m_tapeReclaimer );
        addShutdownListener( m_tapeEjector );
        addShutdownListener( m_tapeEjectorDismount );
        addShutdownListener( m_tapeOnliner );
        addShutdownListener( m_tapeForceRemover );
        addShutdownListener( m_tapeVerificationDriver );
        
        LOG.info( getClass().getSimpleName() + " is online and ready." );
    }
    
    
    private void startPreviouslyRunningTasks()
    {
        final TapeService service = m_serviceManager.getService( TapeService.class );
        for ( final Tape t : m_serviceManager.getRetriever( Tape.class ).retrieveAll( 
                Require.beanPropertyEqualsOneOf( Tape.STATE, TapeState.FORMAT_PENDING ) ).toSet() )
        {
            attemptToRestartFormat( t );
        }
        for ( final Tape t : m_serviceManager.getRetriever( Tape.class ).retrieveAll( 
                Require.beanPropertyEqualsOneOf( Tape.STATE, TapeState.FORMAT_IN_PROGRESS ) ).toSet() )
        {
            service.transistState( t, TapeState.UNKNOWN );
            attemptToRestartFormat( t );
        }
        for ( final Tape t : m_serviceManager.getRetriever( Tape.class ).retrieveAll( 
                Require.beanPropertyEqualsOneOf( Tape.STATE, TapeState.IMPORT_PENDING ) ).toSet() )
        {
            service.transistState( t, TapeState.FOREIGN );
            attemptToRestartImport( t );
        }
        for ( final Tape t : m_serviceManager.getRetriever( Tape.class ).retrieveAll( 
                Require.beanPropertyEqualsOneOf( Tape.STATE, TapeState.IMPORT_IN_PROGRESS ) ).toSet() )
        {
            service.transistState( t, TapeState.FOREIGN );
            attemptToRestartImport( t );
        }
        for ( final Tape t : m_serviceManager.getRetriever( Tape.class ).retrieveAll( 
                Require.beanPropertyEqualsOneOf( Tape.STATE, TapeState.RAW_IMPORT_PENDING ) ).toSet() )
        {
            service.transistState( t, TapeState.LTFS_WITH_FOREIGN_DATA );
            attemptToRestartRawImport( t );
        }
        for ( final Tape t : m_serviceManager.getRetriever( Tape.class ).retrieveAll( 
                Require.beanPropertyEqualsOneOf( Tape.STATE, TapeState.RAW_IMPORT_IN_PROGRESS ) ).toSet() )
        {
            service.transistState( t, TapeState.LTFS_WITH_FOREIGN_DATA );
            attemptToRestartRawImport( t );
        }
        for ( final Tape t : m_serviceManager.getRetriever( Tape.class ).retrieveAll( 
                Require.beanPropertyEqualsOneOf( Tape.STATE, TapeState.ONLINE_IN_PROGRESS ) ).toSet() )
        {
            service.transistState( t, TapeState.OFFLINE );
            onlineTape( t.getId() );
        }
        for ( final Tape t : m_serviceManager.getRetriever( Tape.class ).retrieveAll( 
                Require.beanPropertyEqualsOneOf( Tape.STATE, TapeState.EJECT_TO_EE_IN_PROGRESS ) ).toSet() )
        {
            service.rollbackLastStateTransition( t );
            ejectTape( null, t.getId(), null, null );
        }
        
        m_tapeVerifier.schedule();
    }
    
    
    private void attemptToRestartFormat( final Tape tape )
    {
        try
        {
            formatTape( BlobStoreTaskPriority.LOW, tape.getId(), false, false);
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "Failed to restart format for " + tape + ".", ex );
            m_serviceManager.getService( TapeService.class ).transistState( tape, TapeState.UNKNOWN );
        }
    }
    
    
    private void attemptToRestartImport( final Tape tape )
    {
        try
        {
            importTape(
                    BlobStoreTaskPriority.LOW,
                    m_serviceManager.getRetriever( ImportTapeDirective.class ).attain( 
                            ImportTapeTargetDirective.TAPE_ID, tape.getId() ) );
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "Failed to restart import for " + tape + ".", ex );
            m_serviceManager.getService( TapeService.class ).transistState( tape, TapeState.FOREIGN );
        }
    }
    
    
    private void attemptToRestartRawImport( final Tape tape )
    {
        try
        {
            importTape(
                    BlobStoreTaskPriority.LOW,
                    m_serviceManager.getRetriever( RawImportTapeDirective.class ).attain( 
                            ImportTapeTargetDirective.TAPE_ID, tape.getId() ) );
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "Failed to restart import for " + tape + ".", ex );
            m_serviceManager.getService( TapeService.class ).transistState(
                    tape, TapeState.LTFS_WITH_FOREIGN_DATA );
        }
    }
    
    
    private final static class TapePartitionResourceProvider
        implements CacheResultProvider< String, TapePartitionResource >
    {
        private TapePartitionResourceProvider( final RpcClient rpcClient )
        {
            m_rpcClient = rpcClient;
        }

        public TapePartitionResource generateCacheResultFor( final String tapePartitionSerialNumber )
        {
            return m_rpcClient.getRpcResource( 
                    TapePartitionResource.class, 
                    tapePartitionSerialNumber,
                    ConcurrentRequestExecutionPolicy.SERIALIZED );
        }
        
        private final RpcClient m_rpcClient;
    } // end inner class def
    
    
    private TapeEnvironmentResource createTapeEnvironmentResource( final RpcClient rpcClient )
    {
        return rpcClient.getRpcResource( 
                TapeEnvironmentResource.class,
                null,
                ConcurrentRequestExecutionPolicy.CONCURRENT );
    }
    
    
    public void importTape( 
            final BlobStoreTaskPriority priority,
            final ImportTapeDirective directive )
    {
        Validations.verifyNotNull( "Directive", directive );
        Validations.verifyNotNull( "Tape", directive.getTapeId() );
        synchronized ( m_processor.getTaskStateLock() )
        {
            importTapeInternal( priority, directive );
        }
    }
    
    
    private void validateABMConfigForImport(
            final BlobStoreTaskPriority priority,
            final ImportTapeTargetDirective< ? > directive )
    {
        final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( directive.getTapeId() );
        WhereClause storageDomainFilter = Require.nothing();
        WhereClause dataPersistenceRuleFilter = Require.nothing();  
        if ( null != directive.getStorageDomainId() )
        {
            storageDomainFilter = Require.beanPropertyEquals(
                    Identifiable.ID,
                    directive.getStorageDomainId() );
                    
        }
        if ( null != directive.getDataPolicyId() ) {
            dataPersistenceRuleFilter = Require.beanPropertyEquals(
                    DataPlacement.DATA_POLICY_ID,
                    directive.getDataPolicyId());
            final boolean iomEnabled = m_serviceManager.getRetriever(DataPathBackend.class)
                    .attain(Require.nothing()).isIomEnabled();
            final DataPolicyRM policy = new DataPolicyRM(directive.getDataPolicyId(), m_serviceManager);
            final int totalCopies = policy.getAzureDataReplicationRules().toList().size()
                    + policy.getS3DataReplicationRules().toList().size()
                    + policy.getDs3DataReplicationRules().toList().size()
                    + ((Long) policy.getDataPersistenceRules().toList().stream()
                        .filter((it) -> it.getType() == DataPersistenceRuleType.PERMANENT).count()).intValue();
            if (iomEnabled && totalCopies > 1) {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Data policy \"" + policy.getName() + "\" maintains " + totalCopies + " of data" +
                                " between all rules. Please disable IOM while performing tape imports into policies" +
                                " that have multiple copies.");
            }
        }
        final int validStorageDomainMembers =
                m_serviceManager.getRetriever( StorageDomainMember.class ).getCount(
                        Require.all(
                                Require.beanPropertyEquals(
                                        StorageDomainMember.TAPE_PARTITION_ID,
                                        tape.getPartitionId() ),
                                Require.beanPropertyEquals(
                                        StorageDomainMember.TAPE_TYPE,
                                        tape.getType() ),
                                Require.exists(
                                        StorageDomainMember.STORAGE_DOMAIN_ID,
                                        Require.all(
                                                storageDomainFilter,
                                                Require.exists(
                                                        DataPersistenceRule.class,
                                                        DataPersistenceRule.STORAGE_DOMAIN_ID,
                                                        dataPersistenceRuleFilter ) ) ) ) );
        if ( 0 == validStorageDomainMembers )
        {
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "Could not determine a valid import configuration for tape "
                    + tape.getBarCode() + ". Please ensure data persistence rules"
                    + " and storage domain members are configured correctly for"
                    + " the data policy and storage domain you wish to import to." );    
        }
    }
    
    
    private void importTapeInternal(
            final BlobStoreTaskPriority priority,
            final ImportTapeDirective directive )
    {
        verifyTapeNotLocked( directive.getTapeId() );
        final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( directive.getTapeId() );
        if ( TapeState.FOREIGN != tape.getState() )
        {
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "Cannot import a tape in state " + tape.getState() + "." );
        }

        validateABMConfigForImport(priority, directive);

        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            transaction.getService( TapeService.class ).transistState( tape, TapeState.IMPORT_PENDING );
            transaction.getService( ImportTapeDirectiveService.class ).deleteByEntityToImport(
                    directive.getTapeId() );
            transaction.getService( ImportTapeDirectiveService.class ).create( directive );

            m_processor.getTapeTasks().addStaticTask(new ImportTapeTask( priority,
                    directive.getTapeId(),
                    this,
                    m_diskManager,
                    m_tapeFailureManagement,
                    m_serviceManager ));
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }


    public void importTape(
            final BlobStoreTaskPriority priority,
            final RawImportTapeDirective directive )
    {
        Validations.verifyNotNull( "Directive", directive );
        Validations.verifyNotNull( "Tape", directive.getTapeId() );
        synchronized ( m_processor.getTaskStateLock() )
        {
            importTapeInternal( priority, directive );
        }
    }


    private void importTapeInternal(
            final BlobStoreTaskPriority priority,
            final RawImportTapeDirective directive )
    {
        verifyTapeNotLocked( directive.getTapeId() );
        final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( directive.getTapeId() );
        if ( TapeState.LTFS_WITH_FOREIGN_DATA != tape.getState() )
        {
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "Cannot import a tape in state " + tape.getState() + "." );
        }
        else if ( !tape.isWriteProtected() )
        {
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "Raw imports require that the tape be write protected." );
        }

        validateABMConfigForImport(priority, directive);

        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            transaction.getService( TapeService.class ).transistState( tape, TapeState.RAW_IMPORT_PENDING );
            transaction.getService( RawImportTapeDirectiveService.class ).deleteByEntityToImport(
                    directive.getTapeId() );
            transaction.getService( RawImportTapeDirectiveService.class ).create( directive );

            m_processor.getTapeTasks().addStaticTask(
                    new RawImportTapeTask( priority,
                            directive.getTapeId(),
                            this,
                            m_diskManager,
                            m_tapeFailureManagement,
                            m_serviceManager ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }


    public void cancelImportTape( final UUID tapeId )
    {
        synchronized ( m_processor.getTaskStateLock() )
        {
            verifyTapeNotLocked( tapeId );
            final List<TapeTask> tasks = m_processor.getTapeTasks().get( tapeId );
            tasks.removeIf( task -> !BaseImportTapeTask.class.isAssignableFrom( task.getClass() ) );
            if ( tasks.isEmpty() )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "No import task scheduled for tape." );
            }

            tasks.forEach(task -> {
                if ( BlobStoreTaskState.READY != task.getState() )
                {
                    throw new DataPlannerException(
                            GenericFailure.CONFLICT,
                            "Cannot cancel import in progress (task is in state " + task.getState() + ")." );
                }
            });

            final BeansServiceManager transaction = m_serviceManager.startTransaction();
            try
            {
                transaction.getService( ImportTapeDirectiveService.class )
                    .deleteByEntityToImport( tapeId );
                transaction.getService( RawImportTapeDirectiveService.class )
                    .deleteByEntityToImport( tapeId );
                transaction.getService( TapeService.class ).rollbackLastStateTransition(
                        m_serviceManager.getRetriever( Tape.class ).attain( tapeId ) );
                transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }

            tasks.forEach(task -> m_processor.getTapeTasks().remove( task, "cancel requested" ));
        }
    }


    public void inspectTape( final BlobStoreTaskPriority priority, final UUID tapeId )
    {
        final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
        final String error = m_processor.scheduleInspection( priority, tapeId, true );
        if ( null != error )
        {
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "Failed to schedule inspection for " + tape.getBarCode() + " since " + error + "." );
        }
    }


    public void verify(@NonNull final BlobStoreTaskPriority priority, final UUID tapeId )
    {
        final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
        Validations.verifyNotNull( "TapeId", tapeId );
        if ( TapeState.NORMAL != tape.getState() )
        {
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "Cannot verify a tape in state " + tape.getState() + "." );
        }
        if ( !tape.getType().canContainData() )
        {
            throw new DataPlannerException(
                    GenericFailure.BAD_REQUEST,
                    "Cannot verify a tape that cannot contain data." );
        }
        if ( null == tape.getStorageDomainMemberId() )
        {
            throw new DataPlannerException(
                    GenericFailure.BAD_REQUEST,
                    "Cannot verify a tape that isn't assigned to a storage domain." );
        }

        m_serviceManager.getService( TapeService.class ).update(
                tape.setVerifyPending( priority ),
                Tape.VERIFY_PENDING );
        m_tapeVerifier.schedule();
    }


    public void cleanDrive( final UUID driveId )
    {
        final String retval = m_processor.scheduleCleaning( driveId );
        if ( null != retval )
        {
            throw new DataPlannerException( GenericFailure.CONFLICT, retval );
        }
    }


    public void testDrive(final UUID driveId, final UUID tapeId, boolean cleanFirst)
    {
        final String retval = m_processor.scheduleTest( driveId, tapeId, cleanFirst );
        if ( null != retval )
        {
            throw new DataPlannerException( GenericFailure.CONFLICT, retval );
        }
    }


    public void formatTape(
            final BlobStoreTaskPriority priority,
            final UUID tapeId,
            final boolean force,
            final boolean characterize )
    {
        synchronized ( m_processor.getTaskStateLock() )
        {
            final Tape tape = m_serviceManager.getRetriever( Tape.class )
                                              .attain( tapeId );
            if ( tape.isWriteProtected() )
            {
                throw new IllegalStateException( "Cannot write to tape since it's write protected." );
            }
            final TapeDriveType driveType =
            		new TapeRM( tape, m_serviceManager ).getPartition().getDriveType();
            if ( !driveType.isWriteSupported( tape.getType() ) )
            {
                throw new IllegalStateException( "Cannot take ownership since tape " + tape.getId()
                	+ " ("  + tape.getBarCode() + ") is of type " + tape.getType() + " and is not writeable in"
            			+ " a partition with drives of type " + driveType + ".");
            }
            formatTapeInternal( priority, tapeId, force, characterize );
        }
    }


    private void formatTapeInternal(
            final BlobStoreTaskPriority priority,
            final UUID tapeId,
            final boolean force,
            final boolean characterize )
    {
        Validations.verifyNotNull( "Tape id", tapeId );
        verifyTapeNotLocked( tapeId );
        final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
        if ( TapeState.FORMAT_PENDING == tape.getState() )
        {
            if ( m_processor.getTapeTasks().tryPriorityUpdate(
                    tapeId, FormatTapeTask.class, priority, false, true ) )
            {
                return;
            }
        }
        else if ( !tape.getState().isTapeCommandAllowed() )
        {
            throw new IllegalStateException(
                    "Cannot format tape '" + tape.getBarCode()
                    + "' while it is in state " + tape.getState() + "." );
        }

        if ( null != tape.getStorageDomainMemberId() )
        {
            if ( m_serviceManager.getRetriever( BlobTape.class ).any( Require.beanPropertyEquals(
                    BlobTape.TAPE_ID, tapeId ) ) )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.CONFLICT,
                        "Cannot format tape '" + tape.getBarCode()
                        + "' since it is allocated to a storage domain and has known data on it." );
            }
        }
        if ( !tape.getType().canContainData() )
        {
            throw new IllegalStateException(
                    "Cannot format tape '" + tape.getBarCode()
                    + "' since it is of type " + tape.getType() + "." );
        }

        if ( !force )
        {
            if ( TapeState.PENDING_INSPECTION == tape.getState() )
            {
                throw new ForceFlagRequiredException(
                        "Cannot format tape '" + tape.getBarCode()
                        + " since it has yet to be inspected." );
            }
            if ( TapeState.NORMAL == tape.getState() )
            {
                throw new ForceFlagRequiredException(
                        "tape is already formatted and normal" );
            }
            if ( TapeState.FOREIGN == tape.getState() )
            {
                throw new ForceFlagRequiredException(
                        "tape is foreign and contains data (it should probably be imported)" );
            }
        }

        m_serviceManager.getService( TapeService.class ).transistState( tape, TapeState.FORMAT_PENDING );
        m_processor.getTapeTasks().addStaticTask(
                new FormatTapeTask( priority, tapeId, characterize, m_tapeFailureManagement, m_serviceManager ) );
    }


    private void verifyTapeNotLocked( final UUID tapeId )
    {
        final Object lockHolder = m_tapeEnvironment.getTapeLockHolder( tapeId );
        if ( null == lockHolder )
        {
            return;
        }

        throw new DataPlannerException(
                GenericFailure.CONFLICT,
                "Tape " + tapeId + " is currently locked by " + lockHolder + "." );
    }


    public void ejectTape(
            final BlobStoreTaskPriority verifyPriorToAutoEject,
            final UUID tapeId,
            final String ejectLabel,
            final String ejectLocation )
    {
        if ( null != verifyPriorToAutoEject )
        {
            verify( verifyPriorToAutoEject, tapeId );
        }
        synchronized ( m_processor.getTaskStateLock() )
        {
            ejectTapeInternal( tapeId, ejectLabel, ejectLocation );
        }
    }


    private void ejectTapeInternal( final UUID tapeId, final String ejectLabel, final String ejectLocation )
    {
        final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
        if ( null != tape.getStorageDomainMemberId() )
        {
            final StorageDomain storageDomain =
                    new TapeRM( tape, m_serviceManager ).getStorageDomainMember().getStorageDomain().unwrap();
            if ( !storageDomain.isMediaEjectionAllowed() )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.CONFLICT,
                        "Tape " + tapeId + " (" + tape.getBarCode()
                        + ") has been allocated to storage domain "
                        + storageDomain.getName() + ", which does not permit exporting of media." );
            }
        }

        final Set< String > tapeAttributesToUpdate = new HashSet<>();
        if ( null != ejectLabel )
        {
            tapeAttributesToUpdate.add( Tape.EJECT_LABEL );
        }
        if ( null != ejectLocation )
        {
            tapeAttributesToUpdate.add( Tape.EJECT_LOCATION );
        }
        if ( !tapeAttributesToUpdate.isEmpty() )
        {
            m_serviceManager.getService( TapeService.class ).update(
                    tape.setEjectLabel( ejectLabel ).setEjectLocation( ejectLocation ),
                    CollectionFactory.toArray( String.class, tapeAttributesToUpdate ) );
        }

        if ( !tape.getState().isPhysicallyPresent() )
        {
            return;
        }

        m_serviceManager.getService( TapeService.class ).update(
                tape.setEjectPending( new Date() ),
                Tape.EJECT_PENDING );

        if ( 0 == m_serviceManager.getRetriever( Tape.class )
                                  .getCount( Require.all( Require.beanPropertyEquals( Identifiable.ID, tapeId ),
                                          Require.exists( TapeDrive.class, TapeDrive.TAPE_ID, Require.nothing() ) ) ) )
        {
            m_tapeEjector.schedule();
        }
        else
        {
            m_tapeEjectorDismount.schedule();
        }
    }


    public void cancelEjectTape( final UUID tapeId )
    {
        synchronized ( m_processor.getTaskStateLock() )
        {
            final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
            final TapePartition partition = m_serviceManager.getRetriever( TapePartition.class ).attain( tape.getPartitionId() );
            if ( partition.getImportExportConfiguration() == ImportExportConfiguration.NOT_SUPPORTED )
            {
                throw new DataPlannerException(
                        GenericFailure.BAD_REQUEST,
                        "Cannot cancel export on tape " + tape.getBarCode() + " because tape partition " + partition.getId()
                                + " has import export configuration " + partition.getImportExportConfiguration()
                                + " which does not support canceling tape exports." );
            }
            if ( null == tape.getEjectPending() && TapeState.EJECT_FROM_EE_PENDING != tape.getState() )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Tape is either export-in-progress or not scheduled for export.  Tape state: "
                        + tape.getState() );
            }
            if ( null == tape.getEjectPending() )
            {
                m_serviceManager.getService( TapeService.class ).transistState(
                        tape,
                        TapeState.OFFLINE );
                onlineTapeInternal( tape.getId() );
            }
            else
            {
                m_serviceManager.getService( TapeService.class ).update(
                        tape.setEjectPending( null ),
                        Tape.EJECT_PENDING );
            }
        }
    }


    public void onlineTape( final UUID tapeId )
    {
        synchronized ( m_processor.getTaskStateLock() )
        {
            onlineTapeInternal( tapeId );
        }
    }


    private void onlineTapeInternal( final UUID tapeId )
    {
        final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
        if ( TapeState.ONLINE_PENDING == tape.getState()
                || TapeState.ONLINE_IN_PROGRESS == tape.getState() )
        {
            return;
        }
        if ( TapeState.OFFLINE != tape.getState() )
        {
            throw new DataPlannerException(
                    GenericFailure.BAD_REQUEST,
                    "Cannot online tape '" + tapeId + "' while it is in state " + tape.getState() + "." );
        }

        m_serviceManager.getService( TapeService.class ).transistState( tape, TapeState.ONLINE_PENDING );
        m_tapeOnliner.schedule();
    }


    public void cancelOnlineTape( final UUID tapeId )
    {
        synchronized ( m_processor.getTaskStateLock() )
        {
            final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
            if ( TapeState.ONLINE_PENDING != tape.getState() )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Tape is either online-in-progress or not scheduled for onlining.  Tape state: "
                        + tape.getState() );
            }
            m_serviceManager.getService( TapeService.class ).transistState( tape, TapeState.OFFLINE );
        }
    }
    
    
    public void refreshEnvironmentNow()
    {
        m_tapeEnvironment.ensurePhysicalTapeEnvironmentUpToDate( Refresh.FORCED );
    }


    public void flagEnvironmentForRefresh()
    {
        m_tapeEnvironment.flagForRefresh();
    }


    @Override
    public void driveDump(final UUID driveId) {
        m_processor.driveDump(driveId);
    }


    @Override
    public void taskSchedulingRequired() {
        m_processor.taskSchedulingRequired();
    }
    
    
    public boolean hasIoTask( final UUID chunkId )
    {
        synchronized ( m_processor.getTaskStateLock() )
        {
        	if ( m_processor.getTapeTasks().getChunkIds().contains( chunkId ) )
            {
                return true;
            }
            return false;
        }
    }

    
    public void cancelFormatTape( final UUID tapeId )
    {
        synchronized ( m_processor.getTaskStateLock() )
        {
            verifyTapeNotLocked( tapeId );
            final List<TapeTask> tasks = m_processor.getTapeTasks().get( tapeId );
            tasks.removeIf(task -> !FormatTapeTask.class.isAssignableFrom( task.getClass() ) );
            if ( tasks.isEmpty() )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "No format task scheduled for tape." );
            }

            tasks.forEach(task -> {
                if ( BlobStoreTaskState.READY != task.getState() )
                {
                    throw new DataPlannerException(
                            GenericFailure.CONFLICT,
                            "Cannot cancel format in progress (task is in state " + task.getState() + ")." );
                }
            });

            
            m_serviceManager.getService( TapeService.class ).rollbackLastStateTransition( 
                    m_serviceManager.getRetriever( Tape.class ).attain( tapeId ) );

            tasks.forEach(task -> m_processor.getTapeTasks().remove( task, "cancel requested" ));
        }
    }
    
    
    public void cancelVerifyTape( final UUID tapeId )
    {
        final Tape tape = m_serviceManager.getRetriever( Tape.class ).attain( tapeId );
        synchronized ( m_processor.getTaskStateLock() )
        {
            final List<TapeTask> tasks = m_processor.getTapeTasks().get( tapeId );
            tasks.removeIf( task -> !VerifyTapeTask.class.isAssignableFrom( task.getClass() ) );
            tasks.forEach(task -> {
                if ( BlobStoreTaskState.READY != task.getState() )
                {
                    throw new DataPlannerException(
                            GenericFailure.CONFLICT,
                            "Cannot cancel verify in progress (task is in state " + task.getState() + ")." );
                }
                m_processor.getTapeTasks().remove( task, "cancel requested" );
            });

            m_serviceManager.getService( TapeService.class ).update(
                    tape.setVerifyPending( null ), Tape.VERIFY_PENDING );
        }
    }


    public void cancelTestDrive( final UUID driveId )
    {
        synchronized ( m_processor.getTaskStateLock() )
        {
            final List<TapeTask> tasks = m_processor.getTapeTasks().getAllTapeTasks();
            //We remove either a clean or a test here
            tasks.removeIf( task -> !TestTapeDriveTask.class.isAssignableFrom( task.getClass() ) && !CleanTapeDriveTask.class.isAssignableFrom( task.getClass() ) );
            tasks.forEach(task -> {
                if (driveId == null || driveId.equals(task.getDriveId())) {
                    if ( BlobStoreTaskState.READY != task.getState() )
                    {
                        throw new DataPlannerException(
                                GenericFailure.CONFLICT,
                                "Cannot cancel task in progress (task is in state " + task.getState() + ")." );
                    }
                }

                m_processor.getTapeTasks().remove( task, "cancel requested" );
            });
        }
    }
    
    
    public Set< BlobStoreTask > getTasks()
    {
        synchronized ( m_processor.getTaskStateLock() )
        {
            final Set< BlobStoreTask > retval = new HashSet<>();
            retval.addAll( m_processor.getTapeTasks().getAllTapeTasks() );
            return retval;
        }
    }

    @Override
    public Set<? extends BlobStoreTask> getTasksForJob(final UUID jobId)
    {
        synchronized ( m_processor.getTaskStateLock() )
        {
            return m_processor.getTapeTasks().getTasksForJob(jobId);
        }
    }


    public void deleteOfflineTapePartition( final UUID partitionId ) {
        m_tapeEnvironment.deleteOfflineTapePartition( partitionId );
    }

    public void deleteOfflineTapeDrive( final UUID tapeDriveId ) {
        m_tapeEnvironment.deleteOfflineTapeDrive( tapeDriveId );
    }

    public void deletePermanentlyLostTape( final UUID tapeId ) {
        m_tapeEnvironment.deletePermanentlyLostTape( tapeId );
    }


    /** for testing */ final static String FIELD_PROCESSOR = "m_processor";
    /** for testing */ final static String FIELD_ENVIRONMENT = "m_tapeEnvironment";
    private final SortedSet<LocalBlobDestination> m_writeChunksWaitingToBeAggregated =
    		new TreeSet<>( BeanUtils.getComparator( LocalBlobDestination.class ) );
    private final TapeBlobStoreProcessor m_processor;
    private final BeansServiceManager m_serviceManager;
    private final ReclaimTapeProcessor m_tapeReclaimer;
    private final EjectTapeProcessor m_tapeEjector;
    private final EjectTapeDismountProcessor m_tapeEjectorDismount;
    private final ForceRemovalTapeProcessor m_tapeForceRemover;
    private final OnlineTapeProcessor m_tapeOnliner;
    private final VerifyMediaProcessor< Tape, BlobTape, VerifyTapeTask > m_tapeVerificationDriver;
    private final VerifyTapeProcessor m_tapeVerifier;
    private final JobProgressManager m_jobProgressManager;
    protected final TapeFailureManagement m_tapeFailureManagement;
    final TapeEnvironment m_tapeEnvironment;
    private final DiskManager m_diskManager;

    private final static Logger LOG = Logger.getLogger( TapeBlobStoreImpl.class );
}
