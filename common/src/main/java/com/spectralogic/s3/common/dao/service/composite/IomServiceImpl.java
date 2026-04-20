package com.spectralogic.s3.common.dao.service.composite;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.AzureDataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataMigration;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacement;
import com.spectralogic.s3.common.dao.domain.ds3.DataPlacementRuleState;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.Ds3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.Obsoletion;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMemberState;
import com.spectralogic.s3.common.dao.domain.ds3.WritePreferenceLevel;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.ObsoleteBlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.ObsoleteBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.BlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.common.dao.orm.BlobRM;
import com.spectralogic.s3.common.dao.orm.DataPolicyRM;
import com.spectralogic.s3.common.dao.orm.StorageDomainMemberRM;
import com.spectralogic.s3.common.dao.orm.TapeRM;
import com.spectralogic.s3.common.dao.service.ds3.DataPersistenceRuleService;
import com.spectralogic.s3.common.dao.service.ds3.DegradedBlobService;
import com.spectralogic.s3.common.dao.service.ds3.ObsoletionService;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainMemberService;
import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.common.dao.service.pool.ObsoleteBlobPoolService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.dao.service.tape.BlobTapeService;
import com.spectralogic.s3.common.dao.service.tape.ObsoleteBlobTapeService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.platform.iom.IomUtils;
import com.spectralogic.s3.common.platform.iom.PersistenceProfile;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.iterate.CloseableIterable;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.marshal.DateMarshaler;

public class IomServiceImpl implements IomService
{
    public IomServiceImpl( final BeansServiceManager serviceManager )
    {
        m_serviceManager = serviceManager;
        m_instanceId = m_serviceManager
                .getRetriever( DataPathBackend.class ).attain( Require.nothing() ).getInstanceId();
    }
    
    
    public void cleanupOldMigrations(final Consumer< DataMigration > preDeleteOperation )
    {
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            final Set< DataMigration > oldMigrations = transaction.getRetriever( DataMigration.class ).retrieveAll(
                    Require.all(
                            Require.beanPropertyEquals( DataMigration.PUT_JOB_ID, null),
                            Require.beanPropertyEquals( DataMigration.GET_JOB_ID, null)
                    )).toSet();
            for ( final DataMigration d : oldMigrations )
            {
                preDeleteOperation.accept( d );
                transaction.getDeleter( DataMigration.class ).delete( d.getId() );
            }
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }
    
    
    public Set< DataMigration > getMigrationsInError()
    {
        return m_serviceManager.getRetriever( DataMigration.class ).retrieveAll(
                Require.beanPropertyEquals( DataMigration.IN_ERROR, true) ).toSet();
    }
    
    public void markTapesForAutoCompaction()
    {
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            final Set<Tape> tapes = transaction.getRetriever( Tape.class ).retrieveAll(
                    Require.all(
                            Require.beanPropertyEquals( Tape.STATE, TapeState.NORMAL ),
                            Require.beanPropertyEquals( Tape.FULL_OF_DATA, Boolean.FALSE ),
                            Require.beanPropertyEquals( Tape.WRITE_PROTECTED, Boolean.FALSE ),
                            Require.beanPropertyEquals( Tape.TAKE_OWNERSHIP_PENDING, Boolean.FALSE ),
                            Require.exists(
                            		Tape.PARTITION_ID,
                            		Require.beanPropertyEquals( TapePartition.AUTO_COMPACTION_ENABLED, true ) ),
                            Require.not( Require.beanPropertyEquals(
                                    PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, null) ) ) ).toSet();
                                    
            for ( final Tape tape : tapes )
            {
                if ( null == tape.getAvailableRawCapacity()
                        || null == tape.getTotalRawCapacity()
                        || 0 == tape.getTotalRawCapacity() )
                {
                    continue;
                }
                final long available = tape.getAvailableRawCapacity();
                final long total = tape.getTotalRawCapacity();
                final long used = total - available;
                final long actuallyUsed = transaction.getRetriever( Blob.class ).getSum(
                        Blob.LENGTH,
                        //NOTE: We don't exclude suspect blobs here because a they can't be compacted (deleted) until
                        //they are marked degraded, so they are effectively the same as non-suspect blobs for our
                        //purposes here.
                        Require.exists(
                                BlobTape.class,
                                BlobObservable.BLOB_ID,
                                Require.beanPropertyEquals( BlobTape.TAPE_ID, tape.getId() ) ) );
                final long reclaimableSpace = used - actuallyUsed;
                final Integer threshold =  new TapeRM( tape, transaction )
                        .getStorageDomainMember().getAutoCompactionThreshold();
                if ( null != threshold && reclaimableSpace >  ( threshold * total ) / 100 )
                {
                    transaction.getService( TapeService.class )
                            .transistState( tape, TapeState.AUTO_COMPACTION_IN_PROGRESS );
                }
            }
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }

    public void markSingleTapeForAutoCompaction(final UUID tapeId) {
        if ( !isIomEnabled() ) {
            throw new DaoException( GenericFailure.CONFLICT, "You can only mark a tape for auto compaction if IOM is enabled." );
        }
        final BeansServiceManager transaction = m_serviceManager.startTransaction();

        try
        {
            final Tape tape = transaction.getRetriever( Tape.class ).attain( tapeId );

            if ( tape.getState() == TapeState.AUTO_COMPACTION_IN_PROGRESS ) {
                return;
            }
            if ( tape.getState() != TapeState.NORMAL ) {
                throw new DaoException( GenericFailure.CONFLICT, "Tape " + tapeId +
                        " can not be marked for compaction because its state is " + tape.getState().toString() );
            }
            if ( tape.isWriteProtected() ) {
                throw new DaoException( GenericFailure.CONFLICT, "Tape " + tapeId +
                        " can not be marked for compaction because it is write protected." );
            }
            if ( null == tape.getStorageDomainMemberId() ) {
                throw new DaoException( GenericFailure.CONFLICT, "Tape " + tapeId +
                        " can not be marked for compaction because it does not have a storage domain member." );
            }

            // The partition ID and take ownership check should be skippable if tape state is NORMAL.
            // However, the markTapesForAutoCompaction has these checks, so we are keeping them here for consistency.
            if ( null == tape.getPartitionId() ) {
                throw new DaoException( GenericFailure.CONFLICT, "Tape " + tapeId +
                        " can not be marked for compaction because it does not belong to a tape partition." );
            }
            final TapePartition tapePartition = transaction.getRetriever( TapePartition.class ).attain( tape.getPartitionId() );
            if ( !tapePartition.isAutoCompactionEnabled() ) {
                throw new DaoException( GenericFailure.CONFLICT, "Tape " + tapeId +
                        " can not be marked for compaction because it belongs to tape partition " +
                        tape.getPartitionId() + " which has auto compaction disabled." );
            }
            if ( tape.isTakeOwnershipPending() ) {
                throw new DaoException( GenericFailure.CONFLICT, "Tape " + tapeId +
                        " can not be marked for compaction because take ownership is pending.");
            }

            transaction.getService( TapeService.class ).transistState( tape, TapeState.AUTO_COMPACTION_IN_PROGRESS );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }
    
    public void markTapesFinishedAutoCompacting()
    {
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            final Set<Tape> tapes = transaction.getRetriever( Tape.class ).retrieveAll(
                    Require.all(
                            Require.beanPropertyEquals(
                                    Tape.STATE,
                                    TapeState.AUTO_COMPACTION_IN_PROGRESS ),
                            Require.not(
                                    Require.exists( BlobTape.class,
                                            BlobTape.TAPE_ID,
                                            Require.nothing() ) ) ) ).toSet();
                                    
            for ( final Tape tape : tapes )
            {
                transaction.getService( TapeService.class )
                        .transistState(tape, TapeState.NORMAL);
            }
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }
    
    
    //NOTE: method assumes any blobs passed here belong to the same data policy
    public void removeDegradedBlobsThatHaveBeenHealed(final Collection< UUID > blobIds )
    {
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            if ( null != blobIds )
            {
                final UUID dataPolicyId = new BlobRM ( blobIds.iterator().next(), transaction )
                        .getObject().getBucket().getDataPolicy().getId();
                removeDegradedBlobsThatHaveBeenHealed( dataPolicyId, blobIds, transaction );
            }
            else
            {
                for ( final DataPolicy dataPolicy : transaction.getRetriever( DataPolicy.class ).retrieveAll().toSet())
                {
                    removeDegradedBlobsThatHaveBeenHealed( dataPolicy.getId(), null, transaction );
                }
            }
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }
    
    
    private static void removeDegradedBlobsThatHaveBeenHealed(
            final UUID dataPolicyId,
            final Collection< UUID > blobIds,
            final BeansServiceManager transaction )
    {
        final Set< DataPersistenceRule > rules =
                new DataPolicyRM( dataPolicyId, transaction ).getDataPersistenceRules().toSet();
        final Set< AzureDataReplicationRule > azureRules =
                new DataPolicyRM( dataPolicyId, transaction ).getAzureDataReplicationRules().toSet();
        final Set< S3DataReplicationRule > s3Rules =
                new DataPolicyRM( dataPolicyId, transaction ).getS3DataReplicationRules().toSet();
        final Set< Ds3DataReplicationRule > ds3Rules =
                new DataPolicyRM( dataPolicyId, transaction ).getDs3DataReplicationRules().toSet();

        WhereClause blobFilter = Require.nothing();
        if ( null != blobIds )
        {
            blobFilter = Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds);
        }
        for ( final DataPersistenceRule rule : rules )
        {
            final WhereClause persistenceFilter = getPersistenceFilterForRule(rule);
            final WhereClause ruleFilter = Require.beanPropertyEquals( DegradedBlob.PERSISTENCE_RULE_ID, rule.getId() );
            clearDegradedBlobs(transaction, blobFilter, persistenceFilter, ruleFilter);
        }
        for ( final AzureDataReplicationRule rule : azureRules )
        {
            final WhereClause persistenceFilter = getPersistenceFilterForReplicationRule( BlobAzureTarget.class, SuspectBlobAzureTarget.class, rule);
            final WhereClause ruleFilter = Require.beanPropertyEquals( DegradedBlob.AZURE_REPLICATION_RULE_ID, rule.getId() );
            clearDegradedBlobs(transaction, blobFilter, persistenceFilter, ruleFilter);
        }
        for ( final S3DataReplicationRule rule : s3Rules )
        {
            final WhereClause persistenceFilter = getPersistenceFilterForReplicationRule( BlobS3Target.class, SuspectBlobS3Target.class, rule);
            final WhereClause ruleFilter = Require.beanPropertyEquals( DegradedBlob.S3_REPLICATION_RULE_ID, rule.getId() );
            clearDegradedBlobs(transaction, blobFilter, persistenceFilter, ruleFilter);
        }
        for ( final Ds3DataReplicationRule rule : ds3Rules )
        {
            final WhereClause persistenceFilter = getPersistenceFilterForReplicationRule( BlobDs3Target.class, SuspectBlobDs3Target.class, rule);
            final WhereClause ruleFilter = Require.beanPropertyEquals( DegradedBlob.DS3_REPLICATION_RULE_ID, rule.getId() );
            clearDegradedBlobs(transaction, blobFilter, persistenceFilter, ruleFilter);
        }
    }

    private static void clearDegradedBlobs( final BeansServiceManager transaction, final WhereClause blobFilter, final WhereClause persistenceFilter, final WhereClause ruleFilter) {
        final WhereClause deleteFilter = Require.all(
                ruleFilter,
                blobFilter,
                persistenceFilter);
        transaction.getService(DegradedBlobService.class).delete(deleteFilter);
    }

    private static WhereClause getPersistenceFilterForRule( final DataPersistenceRule rule ) {
        return Require.exists(
                BlobObservable.BLOB_ID,
                IomUtils.blobPersistedToStorageDomain(
                        rule.getStorageDomainId(), PersistenceProfile.DATA_INTEGRITY_OK ) );
    }

    private static < T extends DatabasePersistable & BlobTarget<?>, S extends T > WhereClause getPersistenceFilterForReplicationRule(
            final Class< T > blobTargetType,
            final Class< S > suspectBlobTargetType,
            final DataReplicationRule< ? > rule) {
        return Require.exists(
                BlobObservable.BLOB_ID,
                IomUtils.blobPersistedToTarget(
                        blobTargetType,
                        suspectBlobTargetType,
                        rule.getTargetId(), PersistenceProfile.DATA_INTEGRITY_OK ) );
    }

    public void handleDataPlacementRulesFinishedPendingInclusion()
    {
    	handlePersistenceRulesFinishedPendingInclusion();
    	handleReplicationRulesFinishedPendingInclusion(
    			Ds3DataReplicationRule.class,
    			BlobDs3Target.class,
    			SuspectBlobDs3Target.class );
    	handleReplicationRulesFinishedPendingInclusion(
    			S3DataReplicationRule.class,
    			BlobS3Target.class,
    			SuspectBlobS3Target.class );
    	handleReplicationRulesFinishedPendingInclusion(
    			AzureDataReplicationRule.class,
    			BlobAzureTarget.class,
    			SuspectBlobAzureTarget.class );
    }
    
    
    private void handlePersistenceRulesFinishedPendingInclusion()
    {
        synchronized ( m_dataPersistenceRuleLock )
        {
            final Set< DataPersistenceRule > dataPersistenceRules =
                    m_serviceManager.getRetriever( DataPersistenceRule.class ).retrieveAll(
                            Require.beanPropertyEquals(
                                    DataPlacement.STATE,
                                    DataPlacementRuleState.INCLUSION_IN_PROGRESS ) ).toSet();        
            for ( final DataPersistenceRule rule : dataPersistenceRules)
            {
                final BeansServiceManager transaction = m_serviceManager.startTransaction();
                try
                {
                    //NOTE: we won't hold up considering the rule "included" just because some of its data is pending
                    //migration to elsewhere within the same storage domain (member exclusion or auto-compaction). 
                    final boolean anyBlobsStillPendingPersistence =
                            transaction.getRetriever( Blob.class ).any(
                                    Require.all(
                                            IomUtils.blobNotPartOfAPutJob( false ),
                                            IomUtils.blobBelongsToDataPolicy(rule.getDataPolicyId() ),
                                            Require.not(
                                                    IomUtils.blobPersistedToStorageDomain(
                                                            rule.getStorageDomainId(),
                                                            PersistenceProfile.DATA_INTEGRITY_OK ) ) ) );
                    if ( !anyBlobsStillPendingPersistence )
                    {
                        transaction.getService( DataPersistenceRuleService.class )
                                .update(rule.setState(DataPlacementRuleState.NORMAL), DataPlacement.STATE);
                        LOG.info( "Inclusion of persistence rule " + rule.getId() + " is complete.");
                    }
                    transaction.commitTransaction();
                }
                finally
                {
                    transaction.closeTransaction();
                }
            }
        }
    }
    
    
    private < R extends DataReplicationRule< R > & DatabasePersistable,
    		 T extends DatabasePersistable & BlobTarget< ? >,
    		 S extends T >
			    	void handleReplicationRulesFinishedPendingInclusion(
			    			final Class< R > ruleType,
					    	final Class< T > blobTargetType,
							final Class< S > suspectBlobTargetType )
    {
        synchronized ( m_dataPersistenceRuleLock )
        {
            final Set< R > replicationRules =
                    m_serviceManager.getRetriever( ruleType ).retrieveAll(
                            Require.beanPropertyEquals(
                                    DataPlacement.STATE,
                                    DataPlacementRuleState.INCLUSION_IN_PROGRESS ) ).toSet();        
            for ( final R rule : replicationRules)
            {
                final BeansServiceManager transaction = m_serviceManager.startTransaction();
                try
                {
                    final boolean anyBlobsStillPendingPersistence =
                            transaction.getRetriever( Blob.class ).any(
                                    Require.all(
                                            IomUtils.blobNotPartOfAPutJob( false ),
                                            IomUtils.blobBelongsToDataPolicy(rule.getDataPolicyId() ),
                                            Require.not(
                                                    IomUtils.blobPersistedToTarget(
                                                    		blobTargetType,
                                                    		suspectBlobTargetType,
                                                    		rule.getTargetId(),
                                                    		PersistenceProfile.DATA_INTEGRITY_OK ) ) ) );
                    if ( !anyBlobsStillPendingPersistence )
                    {
                        transaction.getUpdater( ruleType )
                                .update( rule.setState(DataPlacementRuleState.NORMAL), DataPlacement.STATE);
                        LOG.info( "Inclusion of replication rule " + rule.getId() + " is complete.");
                    }
                    transaction.commitTransaction();
                }
                finally
                {
                    transaction.closeTransaction();
                }
            }
        }
    }
    
    
    public void handleStorageDomainMembersFinishedPendingExclusion()
    {
        synchronized ( m_storageDomainMemberLock )
        {
        	final BeansServiceManager transaction = m_serviceManager.startTransaction();
        	try
	        	{
	            final Set< StorageDomainMember > membersToDelete =
	                    m_serviceManager.getRetriever( StorageDomainMember.class )
	                            .retrieveAll( Require.all(
	                                    Require.beanPropertyEquals(
	                                            StorageDomainMember.STATE,
	                                            StorageDomainMemberState.EXCLUSION_IN_PROGRESS ),
	                                    Require.not(
	                                            Require.exists(
	                                                    Tape.class,
	                                                    PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
	                                                    Require.exists(
	                                                            BlobTape.class,
	                                                            BlobTape.TAPE_ID,
	                                                            Require.nothing() ) ) ),
	                                    Require.not(
	                                            Require.exists(
	                                                    Pool.class,
	                                                    PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
	                                                    Require.exists(
	                                                            BlobPool.class,
	                                                            BlobPool.POOL_ID,
	                                                            Require.nothing() ) ) ) ) ).toSet();
	            for ( final StorageDomainMember sdm : membersToDelete )
	            {
	            	final StorageDomainMemberRM sdmRm = new StorageDomainMemberRM( sdm , m_serviceManager );
	                for ( final Tape t : sdmRm.getTapes().toSet() )
	                {
	                	m_serviceManager.getService( TapeService.class ).updateAssignment( t.getId() );
	                }
	                for ( final Pool p : sdmRm.getPools().toSet() )
	                {
	                	m_serviceManager.getService( PoolService.class ).updateAssignment( p.getId() );
	                }
	                if ( new StorageDomainMemberRM( sdm, m_serviceManager ).getStorageDomain().isSecureMediaAllocation() )
	                {
	                	m_serviceManager.getService( StorageDomainMemberService.class ).update(
	                				sdm.setWritePreference( WritePreferenceLevel.NEVER_SELECT )
	                					.setState( StorageDomainMemberState.NORMAL ),
	                				StorageDomainMember.WRITE_PREFERENCE, StorageDomainMember.STATE );
	                }
	                else
	                {
	                	m_serviceManager.getService( StorageDomainMemberService.class ).delete( sdm.getId() );
	                }
	            }
	            transaction.commitTransaction();
        	}
            finally
            {
                transaction.closeTransaction();
            }
        }
    }
    
    
    public void deleteBlobRecordsThatHaveFinishedMigratingOrHealing()
    {
        final Set< StorageDomain > storageDomains =
                m_serviceManager.getRetriever( StorageDomain.class ).retrieveAll().toSet();
        final Date lastBackup = getStartDateOfLastDatabaseBackup();
        if ( null != lastBackup )
        {
            final BeansServiceManager transaction = m_serviceManager.startTransaction();
            try
            {
            for ( final StorageDomain sd : storageDomains )
            {
                if ( !lastBackup.equals( m_lastBackup ) )
                {
                    m_lastBackup = lastBackup;
                    LOG.info( "The new latest backup was started at " + DateMarshaler.marshal( m_lastBackup ) + ". "
                            + "will delete blob records that went obsolete prior to that." );
                    unObsoleteBlobTapesNoLongerSafeToDelete( sd.getId(), transaction );
                    unObsoleteBlobPoolsNoLongerSafeToDelete( sd.getId(), transaction );
                }
            }
            deleteOldObsoletions( lastBackup, transaction );
            transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }
        }
    }
    
    
    private static void unObsoleteBlobTapesNoLongerSafeToDelete(
            final UUID storageDomainId, final BeansServiceManager transaction )
    {
        try ( final CloseableIterable< Set< ObsoleteBlobTape > > blobRecords =
                getObsoleteBlobTapesNoLongerSafeToDelete( storageDomainId, transaction ) )
        {
            for ( final Set< ObsoleteBlobTape > batch : blobRecords )
            {
                transaction.getService( ObsoleteBlobTapeService.class )
                    .delete( BeanUtils.extractPropertyValues( batch, Identifiable.ID ) );
            }
        }
    }
    
    
    private static void unObsoleteBlobPoolsNoLongerSafeToDelete(
            final UUID storageDomainId, final BeansServiceManager transaction )
    {
        try ( final CloseableIterable< Set< ObsoleteBlobPool > > blobRecords =
                getObsoleteBlobPoolsNoLongerSafeToDelete( storageDomainId, transaction ) )
        {
            for ( final Set< ObsoleteBlobPool > batch : blobRecords )
            {
                transaction.getService( ObsoleteBlobPoolService.class )
                    .delete( BeanUtils.extractPropertyValues( batch, Identifiable.ID ) );
            }
        }
    }
    
    
    private static CloseableIterable< Set< ObsoleteBlobTape > > getObsoleteBlobTapesNoLongerSafeToDelete(
            final UUID storageDomainId, final BeansServiceManager transaction )
    {
        return transaction.getRetriever( ObsoleteBlobTape.class )
                //all the blob tapes that
                .retrieveAll( 
                        Require.all(
                                //are on this storage domain
                                IomUtils.blobTapePersistedToStorageDomain(
                                        storageDomainId, PersistenceProfile.ANY ),
                                //but refer to a blob that has no healthy non-obsolete copies anywhere
                                Require.not(
                                        Require.exists(
                                                BlobObservable.BLOB_ID,
                                                IomUtils.blobPersistedToStorageDomain(
                                                        storageDomainId,
                                                        PersistenceProfile.DATA_INTEGRITY_OK_NOT_OBSOLETE ) ) ) ) )
                        .toSetsOf( MAX_BEANS_TO_QUERY );
    }
    
    
    private static CloseableIterable< Set< ObsoleteBlobPool > > getObsoleteBlobPoolsNoLongerSafeToDelete(
            final UUID storageDomainId, final BeansServiceManager transaction )
    {
        return transaction.getRetriever( ObsoleteBlobPool.class )
                //all the blob pools that
                .retrieveAll( 
                        Require.all(
                                //are on this storage domain
                                IomUtils.blobPoolPersistedToStorageDomain(
                                        storageDomainId, PersistenceProfile.ANY ),
                                //but refer to a blob that has no healthy non-obsolete copies anywhere
                                Require.not(
                                        Require.exists(
                                                BlobObservable.BLOB_ID,
                                                IomUtils.blobPersistedToStorageDomain(
                                                        storageDomainId,
                                                        PersistenceProfile.DATA_INTEGRITY_OK_NOT_OBSOLETE ) ) ) ) )
                        .toSetsOf( MAX_BEANS_TO_QUERY );
    }
    
    
    //NOTE: The main function of this call is not just to delete the obsoletion, but more importantly all
    //blob tapes and blob pools that are linked to it.
    private static void deleteOldObsoletions(final Date lastBackup, final BeansServiceManager transaction )
    {
        final Set< Obsoletion > obsoletions = transaction.getService( ObsoletionService.class ).retrieveAll(
                Require.beanPropertyLessThan( Obsoletion.DATE, lastBackup ) ).toSet();
        for ( final Obsoletion o : obsoletions )
        {
            deleteOldBlobTapes( o, transaction );
            deleteOldBlobPools( o, transaction );
        }

        transaction.getService( ObsoletionService.class ).delete(
                Require.beanPropertyLessThan( Obsoletion.DATE, lastBackup ) );
    }


    private static void deleteOldBlobTapes(final Obsoletion obs, final BeansServiceManager transaction )
    {
        final Set< ObsoleteBlobTape > obt = transaction.getService( ObsoleteBlobTapeService.class ).retrieveAll(
                Require.beanPropertyEquals( ObsoleteBlobTape.OBSOLETION_ID, obs.getId() ) ).toSet();

        transaction.getService( BlobTapeService.class ).delete( BeanUtils.extractPropertyValues( obt, Identifiable.ID ) );
    }


    private static void deleteOldBlobPools(final Obsoletion obs, final BeansServiceManager transaction )
    {
        final Set< ObsoleteBlobPool > obp = transaction.getService( ObsoleteBlobPoolService.class ).retrieveAll(
                Require.beanPropertyEquals( ObsoleteBlobPool.OBSOLETION_ID, obs.getId() ) ).toSet();

        transaction.getService( BlobPoolService.class ).delete( BeanUtils.extractPropertyValues( obp, Identifiable.ID ) );
    }
    
    
    private Date getStartDateOfLastDatabaseBackup()
    {
        // Step 1: Find candidate backup properties using a lightweight query that does NOT touch
        // blob/job_entry/job tables. The original single-query approach embedded
        // objectNotPartOfAPutJob() which joins blob(DelayedRunnableRunnerN) x job_entry(N) x job x data_migration
        // in a deeply nested EXISTS. With prepared statement generic plans, PostgreSQL cannot
        // short-circuit on empty results and chooses catastrophic plans (hours for 93K+ rows).
        final Set< S3ObjectProperty > candidateProperties =
                m_serviceManager.getRetriever( S3ObjectProperty.class ).retrieveAll( Require.all(
                        Require.beanPropertyEquals(
                                KeyValueObservable.KEY,
                                KeyValueObservable.BACKUP_START_DATE ),
                        Require.exists(
                                S3ObjectProperty.OBJECT_ID,
                                Require.all(
                                        Require.exists(
                                                S3Object.BUCKET_ID,
                                                Require.beanPropertyMatches( Bucket.NAME, RESERVED_BUCKET_PREFIX ) ),
                                        Require.exists(
                                                S3ObjectProperty.class,
                                                S3ObjectProperty.OBJECT_ID,
                                                Require.all(
                                                        Require.beanPropertyEquals(
                                                                KeyValueObservable.KEY,
                                                                KeyValueObservable.BACKUP_INSTANCE_ID ),
                                                        Require.beanPropertyEquals(
                                                                KeyValueObservable.VALUE,
                                                                m_instanceId.toString() ) ) ) ) ) ) ).toSet();

        // Step 2: For each candidate (typically 0-5 backup objects), check individually whether
        // the object is still being uploaded. This runs the objectNotPartOfAPutJob check per
        // object rather than as a nested subquery, avoiding the bad generic plan.
        final Set< S3ObjectProperty > startDateProperties = new HashSet<>();
        for ( final S3ObjectProperty candidate : candidateProperties )
        {
            final boolean objectStillBeingUploaded = m_serviceManager.getRetriever( Blob.class ).any(
                    Require.all(
                            Require.beanPropertyEquals( Blob.OBJECT_ID, candidate.getObjectId() ),
                            IomUtils.blobPartOfAPutJob( false ) ) );
            if ( !objectStillBeingUploaded )
            {
                startDateProperties.add( candidate );
            }
        }
        Date latest = null;
        for ( final S3ObjectProperty property : startDateProperties )
        {
            try
            {
                
                final String dateString = new String( URLCodec.decodeUrl( property.getValue().getBytes() ) ); 
                final Date backupStartdate = DateMarshaler.unmarshal( dateString );
                if ( null == latest || latest.getTime() < backupStartdate.getTime() )
                {
                    latest = backupStartdate;
                }
            }
            catch ( final RuntimeException | DecoderException e )
            {
                LOG.warn("Was unable to parse database backup date: \"" + property.getValue() + "\"");
            }
            
        }
        return latest;
    }
    
    
    //NOTE: this function is per storage domain - we do not distinguish between temporary and permanent persistence
    //rules at this layer
    public EnhancedIterable< Blob > getBlobsRequiringLocalIOMWork(
            final UUID bucketId,
            final DataPersistenceRule rule )
    {
        final UUID storageDomainId = rule.getStorageDomainId();
        final UUID dataPolicyId = rule.getDataPolicyId();
        final EnhancedIterable< Blob > blobs =
        m_serviceManager.getRetriever( Blob.class ).retrieveAll(
                Require.all(
                        IomUtils.blobNotPartOfAPutJob( true ),
                        IomUtils.blobBelongsInBucket( bucketId ),
                        Require.any(
                                IomUtils.blobIsASuspect( storageDomainId ),
                                IomUtils.blobIsDegraded( rule.getId() ),
                                IomUtils.blobAutoCompactingOrPendingExclusion( storageDomainId ),
                                IomUtils.blobToBeIncluded( storageDomainId, dataPolicyId ) ) )
                ).toIterable();

        return blobs;
    }
    

 	public < T extends DatabasePersistable & BlobTarget< ? >, S extends T > CloseableIterable< Set< Blob > >
    		getBlobsRequiringIOMWorkOnTarget(
    				final Class< T > blobTargetType,
    				final Class< S > suspectBlobTargetType,
		            final UUID bucketId,
		            final UUID targetId,
                    final UUID dataPolicyId )
    {
        final CloseableIterable< Set< Blob > > blobs =
        m_serviceManager.getRetriever( Blob.class ).retrieveAll(
                Require.all(
                        IomUtils.blobNotPartOfAPutJob( true ),
                        IomUtils.blobBelongsInBucket( bucketId ),
                        Require.any(
                        		IomUtils.blobTargetIsASuspect( blobTargetType, suspectBlobTargetType, targetId ),
                                IomUtils.blobTargetIsDegraded( targetId ),
                                IomUtils.blobTargetToBeIncluded( blobTargetType, suspectBlobTargetType, targetId, dataPolicyId ) ) )
                ).toSetsOf( MAX_BLOBS_IN_IOM_JOB );
        return blobs;
    }
    
    
    public Set< DataPersistenceRule > getPermanentPersistenceRulesForBucket( final UUID bucketId )
    {
        return m_serviceManager.getRetriever( DataPersistenceRule.class )
                .retrieveAll( IomUtils.permanentPersistenceRulesForBucket( bucketId ) ).toSet();
    }
    
    
    public boolean isIomEnabled()
    {
    	return m_serviceManager.getRetriever( DataPathBackend.class ).attain( Require.nothing() ).isIomEnabled();
    }
    
    
    public boolean isNewJobCreationAllowed()
    {
    	return m_serviceManager.getRetriever( DataPathBackend.class )
    			.attain( Require.nothing() ).isAllowNewJobRequests();
    }
    

    private final static int MAX_BEANS_TO_QUERY = 100000;
    private final static Object m_storageDomainMemberLock = new Object();
    private final static Object m_dataPersistenceRuleLock = new Object();
    private final static String RESERVED_BUCKET_PREFIX = "Spectra%";
    private final static Logger LOG = Logger.getLogger( IomServiceImpl.class );
    private Date m_lastBackup = null;  
    private final UUID m_instanceId;
    private final BeansServiceManager m_serviceManager;
}
