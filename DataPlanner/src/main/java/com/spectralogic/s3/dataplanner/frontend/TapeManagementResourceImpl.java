/*
 *
 * Copyright C 2019, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.frontend;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.tape.*;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.service.tape.ImportTapeDirectiveService;
import com.spectralogic.s3.common.dao.service.tape.RawImportTapeDirectiveService;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.ImportPersistenceTargetDirectiveRequest;
import com.spectralogic.s3.common.rpc.dataplanner.domain.TapeFailureInformation;
import com.spectralogic.s3.common.rpc.dataplanner.domain.TapeFailuresInformation;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeBlobStore;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.frmwrk.NullAllowed;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseQuiescableRpcResource;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.net.rpc.server.RpcServer;
import com.spectralogic.util.thread.CronRunnableExecutor;
import com.spectralogic.util.thread.CronRunnableIdentifier;

public final class TapeManagementResourceImpl 
    extends BaseQuiescableRpcResource implements TapeManagementResource
{
    public TapeManagementResourceImpl( 
            final RpcServer rpcServer,
            final TapeBlobStore tapeBlobStore, 
            final BeansServiceManager serviceManager )
    {
        m_tapeBlobStore = tapeBlobStore;
        m_serviceManager = serviceManager;
        Validations.verifyNotNull( "Tape blob store", tapeBlobStore );
        Validations.verifyNotNull( "Service manager", m_serviceManager );
        
        refreshStorageDomainAutoEjectCronTriggers();
        rpcServer.register( null, this );
    }
    
    
    @Override
    public RpcFuture< TapeFailuresInformation > ejectStorageDomain( 
            final UUID storageDomainId,
            final UUID bucketId,
            final String ejectLabel, 
            final String ejectLocation,
            final UUID [] blobIdsArray )
    {
        final Set< UUID > blobIds = ( null == blobIdsArray || 0 == blobIdsArray.length ) ?
                null
                : CollectionFactory.toSet( blobIdsArray );
        if ( null != blobIds && blobIds.contains( null ) )
        {
            throw new IllegalArgumentException( "Blob ids cannot contain null." );
        }
        final WhereClause filter = Require.all( 
                Require.exists(
                        PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, 
                        Require.beanPropertyEquals( StorageDomainMember.STORAGE_DOMAIN_ID, storageDomainId ) ),
                ( null == blobIds ) ?
                        null
                        : Require.exists( 
                                BlobTape.class,
                                BlobTape.TAPE_ID,
                                Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ) ),
                ( null == bucketId ) ?
                        null 
                        : Require.any( 
                                Require.beanPropertyEquals( PersistenceTarget.BUCKET_ID, bucketId ),
                                Require.exists(
                                        BlobTape.class,
                                        BlobTape.TAPE_ID,
                                        Require.exists( 
                                                BlobObservable.BLOB_ID,
                                                Require.exists( 
                                                        Blob.OBJECT_ID,
                                                        Require.beanPropertyEquals( 
                                                                S3Object.BUCKET_ID,
                                                                bucketId ) ) ) ) ) );
        return ejectTapes( filter, ejectLabel, ejectLocation );
    }
    
    
    private RpcResponse< TapeFailuresInformation > ejectTapes(
            final WhereClause specialConstraints, 
            final String ejectLabel,
            final String ejectLocation )
    {
        final Set< Tape > tapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll( Require.all( 
                specialConstraints,
                Require.not( Require.beanPropertyEqualsOneOf( 
                        Tape.STATE, 
                        TapeState.getStatesThatAreNotPhysicallyPresent() ) ) ) ).toSet();
        return new OperateOnAllTapes( "eject" )
        {
            @Override
            protected void operateOn( final Tape tape )
            {
                ejectTape( tape.getId(), ejectLabel, ejectLocation );
            }
        }.run( tapes );
    }
    

    @Override
    public RpcFuture< TapeFailuresInformation > ejectTape(
            final UUID tapeId,
            final String ejectLabel, 
            final String ejectLocation )
    {
        if ( null == tapeId )
        {
            return ejectTapes( null, ejectLabel, ejectLocation );
        }
        
        m_tapeBlobStore.ejectTape( null, tapeId, ejectLabel, ejectLocation );
        return null;
    }
    

    @Override
    public RpcFuture< TapeFailuresInformation > onlineTape( final UUID tapeId )
    {
        if ( null == tapeId )
        {
            final Set< Tape > tapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll(
                    Tape.STATE, TapeState.OFFLINE ).toSet();
            return new OperateOnAllTapes( "online" )
            {
                @Override
                protected void operateOn( final Tape tape )
                {
                    m_tapeBlobStore.onlineTape( tape.getId() );
                }
            }.run( tapes );
        }
        
        m_tapeBlobStore.onlineTape( tapeId );
        return null;
    }
    

    @Override
    public RpcFuture< TapeFailuresInformation > cancelEjectTape( final UUID tapeId )
    {
        if ( null == tapeId )
        {
            final Set< Tape > tapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll( Require.any( 
                    Require.beanPropertyEquals( Tape.STATE, TapeState.EJECT_FROM_EE_PENDING ),
                    Require.not( Require.beanPropertyEquals( Tape.EJECT_PENDING, null ) ) ) ).toSet();
            return new OperateOnAllTapes( "cancel eject" )
            {
                @Override
                protected void operateOn( final Tape tape )
                {
                    m_tapeBlobStore.cancelEjectTape( tape.getId() );
                }
            }.run( tapes );
        }
        
        m_tapeBlobStore.cancelEjectTape( tapeId );
        return null;
    }
    
    
    @Override
    public RpcFuture< TapeFailuresInformation > cancelFormatTape( final UUID tapeId )
    {
        if ( null == tapeId )
        {
            final Set< Tape > tapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll(
                    Tape.STATE, TapeState.FORMAT_PENDING ).toSet();
            return new OperateOnAllTapes( "cancel format" )
            {
                @Override
                protected void operateOn( final Tape tape )
                {
                    m_tapeBlobStore.cancelFormatTape( tape.getId() );
                }
            }.run( tapes );
        }
        
        m_tapeBlobStore.cancelFormatTape( tapeId );
        return null;
    }
    
    
    @Override
    public RpcFuture< TapeFailuresInformation > cancelImportTape( final UUID tapeId )
    {
        if ( null == tapeId )
        {
            final Set< Tape > tapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll( 
                    Require.beanPropertyEqualsOneOf(
                            Tape.STATE, TapeState.IMPORT_PENDING, TapeState.RAW_IMPORT_PENDING ) ).toSet();
            return new OperateOnAllTapes( "cancel import" )
            {
                @Override
                protected void operateOn( final Tape tape )
                {
                    m_tapeBlobStore.cancelImportTape( tape.getId() );
                }
            }.run( tapes );
        }
        
        m_tapeBlobStore.cancelImportTape( tapeId );
        return null;
    }
    

    @Override
    public RpcFuture< TapeFailuresInformation > cancelOnlineTape( final UUID tapeId )
    {
        if ( null == tapeId )
        {
            final Set< Tape > tapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll( 
                    Tape.STATE, TapeState.ONLINE_PENDING ).toSet();
            return new OperateOnAllTapes( "cancel import" )
            {
                @Override
                protected void operateOn( final Tape tape )
                {
                    m_tapeBlobStore.cancelOnlineTape( tape.getId() );
                }
            }.run( tapes );
        }
        
        m_tapeBlobStore.cancelOnlineTape( tapeId );
        return null;
    }
    

    @Override
    public RpcFuture< TapeFailuresInformation > cancelVerifyTape( final UUID tapeId )
    {
        if ( null == tapeId )
        {
            final Set< Tape > tapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll( 
                    Require.not( Require.beanPropertyEquals(
                            Tape.VERIFY_PENDING, null ) ) ).toSet();
            return new OperateOnAllTapes( "cancel verify" )
            {
                @Override
                protected void operateOn( final Tape tape )
                {
                    m_tapeBlobStore.cancelVerifyTape( tape.getId() );
                }
            }.run( tapes );
        }
        
        m_tapeBlobStore.cancelVerifyTape( tapeId );
        return null;
    }


    @Override
    public RpcFuture< ? > cancelTestDrive(final UUID driveId )
    {
        m_tapeBlobStore.cancelTestDrive(driveId);
        return null;
    }
    
    
    @Override
    public RpcFuture< TapeFailuresInformation > formatTape(final UUID tapeId, final boolean force, boolean characterize)
    {
        if ( null == tapeId )
        {
            final Set< Tape > tapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll( Require.all( 
                    Require.not( Require.beanPropertyEqualsOneOf( 
                            Tape.STATE, TapeState.getStatesThatAreNotPhysicallyPresent() ) ),
                    Require.not( Require.beanPropertyEquals( Tape.STATE, TapeState.NORMAL ) ) ) ).toSet();
            return new OperateOnAllTapes( "format" )
            {
                @Override
                protected void operateOn( final Tape tape )
                {
                    m_tapeBlobStore.formatTape( BlobStoreTaskPriority.LOW, tape.getId(), force, characterize );
                }
            }.run( tapes );
        }
        
        m_tapeBlobStore.formatTape( BlobStoreTaskPriority.LOW, tapeId, force, characterize);
        m_tapeBlobStore.taskSchedulingRequired();
        return null;
    }
    
    
    @Override
    public RpcFuture< TapeFailuresInformation > importTape(
            final UUID tapeId,
            final ImportPersistenceTargetDirectiveRequest importDirective )
    {
        verifyNotQuiesced();
        if ( null == tapeId )
        {
            final Set< Tape > tapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll(
                    Tape.STATE, TapeState.FOREIGN ).toSet();
            return new OperateOnAllTapes( "import" )
            {
                @Override
                protected void operateOn( final Tape tape )
                {
                    m_tapeBlobStore.importTape( 
                            importDirective.getPriority(),
                            newImportTapeDirective( tape.getId(), importDirective ) );
                }
            }.run( tapes );
        }
        
        m_tapeBlobStore.importTape( 
                importDirective.getPriority(),
                newImportTapeDirective( tapeId, importDirective ) );
        m_tapeBlobStore.taskSchedulingRequired();
        return null;
    }
    
    
    private ImportTapeDirective newImportTapeDirective( 
            final UUID tapeId,
            final ImportPersistenceTargetDirectiveRequest importDirective )
    {
        final ImportTapeDirective retval = 
                BeanFactory.newBean( ImportTapeDirective.class ).setTapeId( tapeId );
        BeanCopier.copy( retval, importDirective );
        return retval;
    }
    
    
    @Override
    public RpcFuture< TapeFailuresInformation > rawImportTape( 
    		@NullAllowed final UUID tapeId, 
            final UUID bucketId,
            final ImportPersistenceTargetDirectiveRequest importDirective )
    {
        verifyNotQuiesced();
        if ( null == tapeId )
        {
            final Set< Tape > tapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll(
                    Tape.STATE, TapeState.LTFS_WITH_FOREIGN_DATA ).toSet();
            return new OperateOnAllTapes( "raw-import" )
            {
                @Override
                protected void operateOn( final Tape tape )
                {
                    m_tapeBlobStore.importTape( 
                            importDirective.getPriority(),
                            BeanFactory.newBean( RawImportTapeDirective.class )
	                            .setBucketId( bucketId )
	                            .setTapeId( tape.getId() )
	                            .setDataPolicyId( importDirective.getDataPolicyId() )
	                            .setStorageDomainId( importDirective.getStorageDomainId() )
	                            .setUserId( importDirective.getUserId() ) );
                }
            }.run( tapes );
        }
        
        m_tapeBlobStore.importTape( 
        		importDirective.getPriority(),
                BeanFactory.newBean( RawImportTapeDirective.class )
	                .setBucketId( bucketId )
	                .setTapeId( tapeId )
	                .setDataPolicyId( importDirective.getDataPolicyId() )
	                .setStorageDomainId( importDirective.getStorageDomainId() )
	                .setUserId( importDirective.getUserId() ) );
        m_tapeBlobStore.taskSchedulingRequired();
        return null;
    }
    
    
    @Override
    public RpcFuture< TapeFailuresInformation > inspectTape(
            final UUID tapeId, 
            BlobStoreTaskPriority priority )
    {
        if ( null == priority )
        {
            priority = BlobStoreTaskPriority.LOW;
        }
        
        if ( null == tapeId )
        {
            final Set< TapeState > statesCannotInspect = TapeState.getStatesThatAreNotPhysicallyPresent();
            statesCannotInspect.add( TapeState.INCOMPATIBLE );
            final Set< Tape > tapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll(
                    Require.not( Require.beanPropertyEqualsOneOf( Tape.STATE, statesCannotInspect ) ) )
                                                      .toSet();
            final BlobStoreTaskPriority pr = priority;
            return new OperateOnAllTapes( "inspect" )
            {
                @Override
                protected void operateOn( final Tape tape )
                {
                    m_tapeBlobStore.inspectTape( pr, tape.getId() );
                }
            }.run( tapes );
        }
        
        m_tapeBlobStore.inspectTape( priority, tapeId );
        m_tapeBlobStore.taskSchedulingRequired();
        return null;
    }
    
    
    @Override
    public RpcFuture< TapeFailuresInformation > verifyTape(
            final UUID tapeId, 
            BlobStoreTaskPriority priority )
    {
        if ( null == priority )
        {
            priority = BlobStoreTaskPriority.LOW;
        }
        if ( !priority.isSpecifiableByUser() )
        {
            throw new DataPlannerException(
                    GenericFailure.BAD_REQUEST, 
                    "It is illegal to specify priority level " + priority + " as a user." );
        }
        
        if ( null == tapeId )
        {
            final Set< Tape > tapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll( Require.all(
                    Require.beanPropertyEquals( Tape.STATE, TapeState.NORMAL ),
                    Require.not( Require.beanPropertyEquals(
                            PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, null ) ) ) ).toSet();
            final BlobStoreTaskPriority pr = priority;
            return new OperateOnAllTapes( "verify" )
            {
                @Override
                protected void operateOn( final Tape tape )
                {
                    m_tapeBlobStore.verify( pr, tape.getId() );
                }
            }.run( tapes );
        }
        
        m_tapeBlobStore.verify( priority, tapeId );
        m_tapeBlobStore.taskSchedulingRequired();
        return null;
    }
    
    
    @Override
    public RpcFuture< ? > cleanDrive( final UUID driveId )
    {
        m_tapeBlobStore.cleanDrive( driveId );
        m_tapeBlobStore.taskSchedulingRequired();
        return null;
    }


    @Override
    public RpcFuture< ? > testDrive(final UUID driveId, final UUID tapeId, boolean cleanFirst)
    {
        m_tapeBlobStore.testDrive( driveId, tapeId, cleanFirst );
        m_tapeBlobStore.taskSchedulingRequired();
        return null;
    }


    @Override
    public RpcFuture< ? > driveDump(final UUID driveId)
    {
        m_tapeBlobStore.driveDump( driveId );
        return null;
    }
    
    
    @Override
    public RpcFuture< ? > deleteOfflineTapePartition( final UUID partitionId )
    {

        m_tapeBlobStore.deleteOfflineTapePartition( partitionId );
        return null;
    }
    
    
    @Override
    public RpcFuture< ? > deleteOfflineTapeDrive( final UUID tapeDriveId )
    {
        m_tapeBlobStore.deleteOfflineTapeDrive( tapeDriveId );
        return null;
    }


    @Override
    public RpcFuture< ? > deletePermanentlyLostTape( final UUID tapeId )
    {
        m_tapeBlobStore.deletePermanentlyLostTape( tapeId );
        return null;
    }
    
    
    @Override
    public RpcFuture< ? > forceTapeEnvironmentRefresh()
    {
        m_tapeBlobStore.refreshEnvironmentNow();
        return null;
    }


    @Override
    public RpcFuture< ? > flagEnvironmentForRefresh()
    {
        m_tapeBlobStore.flagEnvironmentForRefresh();
        return null;
    }
    
    
    @Override
    protected void forceQuiesceAndPrepareForShutdown()
    {
        cancelImportTape( null );
        m_serviceManager.getService( ImportTapeDirectiveService.class ).deleteAll();
        m_serviceManager.getService( RawImportTapeDirectiveService.class ).deleteAll();
    }


    @Override
    protected String getCauseForNotQuiesced()
    {
        final int tapesBeingImported = m_serviceManager.getRetriever( Tape.class ).getCount( 
                Require.beanPropertyEqualsOneOf(
                        Tape.STATE,
                        TapeState.IMPORT_IN_PROGRESS, TapeState.IMPORT_PENDING ) );
        if ( 0 < tapesBeingImported )
        {
            return tapesBeingImported + " tapes are being imported";
        }
        
        final int importTapeDirectives = 
                m_serviceManager.getRetriever( ImportTapeDirective.class ).getCount();
        if ( 0 < importTapeDirectives )
        {
            return importTapeDirectives + " " + ImportTapeDirective.class.getSimpleName() + "s exist";
        }
            
        return null;
    }
    
    
    @Override
    public RpcFuture< ? > refreshStorageDomainAutoEjectCronTriggers()
    {
        synchronized ( m_storageDomainAutoEjectCronTriggers )
        {
            refreshStorageDomainAutoEjectCronTriggersInternal();
            LOG.info( m_storageDomainAutoEjectCronTriggers.size()
                      + " storage domains have CRON-based auto eject tape triggers." );
            return null;
        }
    }
    
    
    private void refreshStorageDomainAutoEjectCronTriggersInternal()
    {
        final Map< UUID, String > newStorageDomainAutoEjectCronTriggers = new HashMap<>();
        for ( final StorageDomain storageDomain 
                : m_serviceManager.getRetriever( StorageDomain.class ).retrieveAll().toSet() )
        {
            if ( null == storageDomain.getAutoEjectUponCron() )
            {
                continue;
            }
            newStorageDomainAutoEjectCronTriggers.put(
                    storageDomain.getId(),
                    storageDomain.getAutoEjectUponCron() );
            if ( !storageDomain.getAutoEjectUponCron().equals(
                    m_storageDomainAutoEjectCronTriggers.get( storageDomain.getId() ) ) )
            {
                CronRunnableExecutor.schedule(
                        getCronIdentifier( storageDomain.getId() ),
                        storageDomain.getAutoEjectUponCron(),
                        new AutoEjectTapes( storageDomain.getId(), this ) );
            }
        }
        
        final Set< UUID > deletedStorageDomains = 
                new HashSet<>( m_storageDomainAutoEjectCronTriggers.keySet() );
        deletedStorageDomains.removeAll( newStorageDomainAutoEjectCronTriggers.keySet() );
        for ( final UUID storageDomainId : deletedStorageDomains )
        {
            CronRunnableExecutor.unschedule( getCronIdentifier( storageDomainId ) );
        }
        
        m_storageDomainAutoEjectCronTriggers.clear();
        m_storageDomainAutoEjectCronTriggers.putAll( newStorageDomainAutoEjectCronTriggers );
    }
    
    
    /**
     * Package private for testing purposes only.
     */
    CronRunnableIdentifier getCronIdentifier( final UUID storageDomainId )
    {
        return new CronRunnableIdentifier( StorageDomain.class, storageDomainId, AutoEjectTapes.class );
    }
    
    
    private final static class AutoEjectTapes implements Runnable
    {
        private AutoEjectTapes( 
                final UUID storageDomainId, 
                final TapeManagementResource tapeManagementResource )
        {
            m_storageDomainId = storageDomainId;
            m_tapeManagementResource = tapeManagementResource;
        }
        
        
        @Override
        public void run()
        {
            m_tapeManagementResource.ejectStorageDomain(
                    m_storageDomainId, 
                    null, 
                    "Auto-exported via CRON schedule.",
                    null, 
                    null );
        }
        
        
        private final UUID m_storageDomainId;
        private final TapeManagementResource m_tapeManagementResource;
    } // end inner class def
    
    
    private abstract class OperateOnAllTapes
    {
        protected OperateOnAllTapes( final String operationVerb )
        {
            m_operationVerb = operationVerb;
        }
        
        
        final RpcResponse< TapeFailuresInformation > run( final Set< Tape > tapes )
        {
            final List< String > successes = new ArrayList<>();
            for ( final Tape tape : tapes )
            {
                try
                {
                    operateOn( tape );
                    successes.add( tape.getId() + " (" + tape.getBarCode() + ")" );
                }
                catch ( final RuntimeException ex )
                {
                    final String message = 
                            "Cannot " + m_operationVerb + ": " + tape.getId() 
                            + " (" + tape.getBarCode() + ").";
                    LOG.warn( message, ex );
                    m_failures.add( BeanFactory.newBean( TapeFailureInformation.class )
                            .setTapeId( tape.getId() )
                            .setFailure( ExceptionUtil.getReadableMessage( ex ) ) );
                }
            }

            LOG.info( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.convert( m_operationVerb )
                      + " succeeded for " + successes.size() + " tapes and failed for " 
                      + m_failures.size() + " tapes." );
            return new RpcResponse<>( BeanFactory.newBean( TapeFailuresInformation.class )
                    .setFailures( CollectionFactory.toArray( TapeFailureInformation.class, m_failures ) ) );
        }
        
        
        protected abstract void operateOn( final Tape tape );
        
        
        private final String m_operationVerb;
        private final List< TapeFailureInformation > m_failures = new ArrayList<>();
    } // end inner class def
    
    
    private final TapeBlobStore m_tapeBlobStore;
    private final BeansServiceManager m_serviceManager;
    private final Map< UUID, String > m_storageDomainAutoEjectCronTriggers = new HashMap<>();
    
    private final static Logger LOG = Logger.getLogger( TapeManagementResourceImpl.class );
}
