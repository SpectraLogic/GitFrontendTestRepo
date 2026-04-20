/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */

package com.spectralogic.s3.common.platform.iom;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.ObsoleteBlobPool;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.ObsoleteBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;

public class IomUtils
{
    public static WhereClause permanentPersistenceRulesForBucket( final UUID bucketId )
    {
        return Require.all(
                            Require.beanPropertyEquals( DataPersistenceRule.TYPE, DataPersistenceRuleType.PERMANENT ),
                            Require.exists(
                                    DataPlacement.DATA_POLICY_ID,
                                    Require.exists(
                                            Bucket.class,
                                            Bucket.DATA_POLICY_ID,
                                            Require.beanPropertyEquals(Identifiable.ID, bucketId) ) ) );
    }
    
    
    public static WhereClause persistenceRulesForBucket( final UUID bucketId )
    {
        return Require.all(
                Require.beanPropertyEqualsOneOf( DataPersistenceRule.TYPE, DataPersistenceRuleType.PERMANENT,
                        DataPersistenceRuleType.TEMPORARY ), Require.exists( DataPlacement.DATA_POLICY_ID,
                        Require.exists( Bucket.class, Bucket.DATA_POLICY_ID,
                                Require.beanPropertyEquals( Identifiable.ID, bucketId ) ) ) );
    }


    public static WhereClause blobPersistedToStorageDomain (
            final UUID storageDomainId,
            final PersistenceProfile profile )
    {
        return Require.any(
                Require.exists(
                        BlobTape.class,
                        BlobObservable.BLOB_ID,
                        blobTapePersistedToStorageDomain( storageDomainId, profile ) ),
                Require.exists(
                        BlobPool.class,
                        BlobObservable.BLOB_ID,
                        blobPoolPersistedToStorageDomain( storageDomainId, profile ) ) );
    }
    
    
    public static < T extends DatabasePersistable & BlobTarget< ? >, S extends T >WhereClause blobPersistedToTarget ( 
			final Class< T > blobTargetType,
			final Class< S > suspectBlobTargetType,
            final UUID targetId,
            final PersistenceProfile profile )
    {
        return Require.any(
                Require.exists(
                        blobTargetType,
                        BlobObservable.BLOB_ID,
                        blobTargetPersistedToTarget( targetId, suspectBlobTargetType, profile ) ) );
    }
    
    
    public static < T extends BlobTarget<?> & DatabasePersistable > WhereClause blobTargetPersistedToTarget(
            final UUID targetId,
            final Class< T > suspectBlobTargetType,
            PersistenceProfile profile )
    {
        WhereClause suspectFilter = Require.nothing();
        if ( !profile.isIncludeSuspectBlobsRecords() )
        {
            suspectFilter = Require.not(
                                    Require.exists(
                                    		suspectBlobTargetType,
                                            Identifiable.ID,
                                            Require.nothing() ) );
        }
        return Require.all( 
                suspectFilter,
                Require.beanPropertyEquals(
                        BlobTarget.TARGET_ID,
                        targetId ) );
    }


    public static WhereClause blobIsASuspect( final UUID storageDomainId )
    {
        return Require.any(
                        Require.exists(
                                BlobTape.class,
                                BlobObservable.BLOB_ID,
                                Require.all(
                                    Require.exists(
                                            SuspectBlobTape.class,
                                            Identifiable.ID,
                                            Require.nothing() ),
                                    Require.not(
                                    		Require.exists(
                                					ObsoleteBlobTape.class,
                                					Identifiable.ID,
                                					Require.nothing() ) ),
                                    Require.exists(
                                            BlobTape.TAPE_ID,
                                            Require.exists(
                                                    PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                                                    Require.beanPropertyEquals(
                                                            StorageDomainMember.STORAGE_DOMAIN_ID,
                                                            storageDomainId) ) ) ) ),
                        Require.exists(
                                BlobPool.class,
                                BlobObservable.BLOB_ID,
                                Require.all(
		                                Require.exists(
		                                        SuspectBlobPool.class,
		                                        Identifiable.ID,
		                                        Require.nothing() ),
				                        Require.not(
				                        		Require.exists(
				                    					ObsoleteBlobPool.class,
				                    					Identifiable.ID,
				                    					Require.nothing() ) ),
                                        Require.exists(
                                                BlobPool.POOL_ID,
                                                Require.exists(
                                                        PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                                                        Require.beanPropertyEquals(
                                                                StorageDomainMember.STORAGE_DOMAIN_ID,
                                                                storageDomainId) ) ) ) ) );
    }


    public static < T extends DatabasePersistable & BlobTarget< ? >, S extends T > WhereClause blobTargetIsASuspect(
            final Class< T > blobTargetType,
            final Class< S > suspectBlobTargetType,
            final UUID targetId )
    {
        return Require.exists(
                blobTargetType,
                BlobObservable.BLOB_ID,
                Require.all(
                        Require.exists(
                                suspectBlobTargetType,
                                Identifiable.ID,
                                Require.nothing() ),
                        Require.beanPropertyEquals(
                                BlobTarget.TARGET_ID,
                                targetId ) ) );
    }


    public static WhereClause blobIsDegraded( final UUID persistenceRuleId )
    {
        return Require.exists(
                DegradedBlob.class,
                DegradedBlob.BLOB_ID,
                Require.beanPropertyEquals(
                        DegradedBlob.PERSISTENCE_RULE_ID,
                        persistenceRuleId
                ) );
    }


    public static WhereClause blobTargetIsDegraded( final UUID targetId )
    {
        return Require.exists(
                DegradedBlob.class,
                BlobObservable.BLOB_ID,
                Require.any(
                        Require.exists(
                                DegradedBlob.AZURE_REPLICATION_RULE_ID,
                                Require.beanPropertyEquals(
                                        DataReplicationRule.TARGET_ID,
                                        targetId ) ),
                        Require.exists(
                                DegradedBlob.DS3_REPLICATION_RULE_ID,
                                Require.beanPropertyEquals(
                                        DataReplicationRule.TARGET_ID,
                                        targetId ) ),
                        Require.exists(
                                DegradedBlob.S3_REPLICATION_RULE_ID,
                                Require.beanPropertyEquals(
                                        DataReplicationRule.TARGET_ID,
                                        targetId ) ) ) );
    }


    public static WhereClause blobTapePersistedToStorageDomain(
            final UUID storageDomainId,
            PersistenceProfile profile )
    {
        WhereClause autoCompactionFilter = Require.nothing();
        WhereClause tapeErrorStateFilter = Require.nothing();
        WhereClause suspectFilter = Require.nothing();
        WhereClause obsoleteFilter = Require.nothing();
        if ( !profile.isIncludeTapesPendingAutoCompaction() )
        {
            autoCompactionFilter = Require.not( tapeIsAutocompacting() );
        }
        if ( !profile.isIncludeTapesInErrorState() )
        {
            tapeErrorStateFilter = Require.not( tapeIsBad() );
        }
        if ( !profile.isIncludeSuspectBlobsRecords() )
        {
            suspectFilter = Require.not(
                                    Require.exists(
                                            SuspectBlobTape.class,
                                            Identifiable.ID,
                                            Require.nothing() ) );
        }
        if ( !profile.isIncludeObsoleteBlobRecords() )
        {
            obsoleteFilter = Require.not(
                                    Require.exists(
                                            ObsoleteBlobTape.class,
                                            Identifiable.ID,
                                            Require.nothing() ) );
        }
        return Require.all( 
                suspectFilter,
                obsoleteFilter,
                Require.exists(
                        BlobTape.TAPE_ID,
                        Require.all(
                                tapeOrPoolBelongsToStorageDomain(
                                        storageDomainId,
                                        profile.isIncludeMembersPendingExclusion() ),
                                autoCompactionFilter,
                                tapeErrorStateFilter ) ) );
    }
    
    
    public static WhereClause blobPoolPersistedToStorageDomain(
            final UUID storageDomainId,
            final PersistenceProfile profile )
    {
        WhereClause suspectFilter = Require.nothing();
        WhereClause obsoleteFilter = Require.nothing();
        if ( !profile.isIncludeSuspectBlobsRecords() )
        {
            suspectFilter = Require.not(
                                    Require.exists(
                                            SuspectBlobPool.class,
                                            Identifiable.ID,
                                            Require.nothing() ) );
        }
        if ( !profile.isIncludeObsoleteBlobRecords() )
        {
            obsoleteFilter = Require.not(
                                    Require.exists(
                                            ObsoleteBlobPool.class,
                                            Identifiable.ID,
                                            Require.nothing() ) );
        }
        return Require.all(
                suspectFilter,
                obsoleteFilter,
                Require.exists(
                        BlobPool.POOL_ID,
                        tapeOrPoolBelongsToStorageDomain(
                                storageDomainId,
                                profile.isIncludeMembersPendingExclusion() ) ) );
    }


    public static WhereClause blobAutoCompactingOrPendingExclusion(
            final UUID storageDomainId )
    {
        return Require.any(
                        /*
                         * Tape is autocompacting
                         */
                        Require.exists(
                                BlobTape.class,
                                BlobObservable.BLOB_ID,
                                Require.all(
                                		Require.not(
                                        		Require.exists(
                                    					ObsoleteBlobTape.class,
                                    					Identifiable.ID,
                                    					Require.nothing() ) ),
		                                Require.exists(
		                                        BlobTape.TAPE_ID,
		                                        tapeIsAutocompacting() ) ) ),
                        /*
                         * Tape storage domain member is pending exclusion
                         */
                        Require.exists(
                                BlobTape.class,
                                BlobObservable.BLOB_ID,
                                Require.all(
                                		Require.not(
                                        		Require.exists(
                                    					ObsoleteBlobTape.class,
                                    					Identifiable.ID,
                                    					Require.nothing() ) ),
		                                Require.exists(
		                                        BlobTape.TAPE_ID,
		                                        Require.exists(
		                                                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
		                                                Require.all(
		                                                        Require.beanPropertyEquals(
		                                                                StorageDomainMember.STORAGE_DOMAIN_ID,
		                                                                storageDomainId),
		                                                        Require.beanPropertyEquals(
		                                                                StorageDomainMember.STATE,
		                                                                StorageDomainMemberState.EXCLUSION_IN_PROGRESS ) ) ) ) ) ),
                        /*
                         * Pool storage domain member is pending exclusion
                         */
                        Require.exists(
                                BlobPool.class,
                                BlobObservable.BLOB_ID,
                                Require.all(
		                        		Require.not(
		                                		Require.exists(
		                            					ObsoleteBlobPool.class,
		                            					Identifiable.ID,
		                            					Require.nothing() ) ),
		                                Require.exists(
		                                        BlobPool.POOL_ID,
		                                        Require.exists(
		                                                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
		                                                Require.all(
		                                                        Require.beanPropertyEquals(
		                                                                StorageDomainMember.STORAGE_DOMAIN_ID,
		                                                                storageDomainId),
		                                                        Require.beanPropertyEquals(
		                                                                StorageDomainMember.STATE,
		                                                                StorageDomainMemberState.EXCLUSION_IN_PROGRESS ) ) ) ) ) ) );
    }


    public static WhereClause blobToBeIncluded(
            final UUID storageDomainId,
            final UUID dataPolicyId )
    {
        return Require.all(
                /*
                 * Make sure that the blob lives in a data policy with a data persistence rule with inclusion in
                 * progress for the given storage domain.
                 */
                Require.exists(
                        Blob.OBJECT_ID,
                        Require.exists(
                                S3Object.BUCKET_ID,
                                Require.exists(
                                        Bucket.DATA_POLICY_ID,
                                        Require.all(
                                                Require.beanPropertyEquals(
                                                        Identifiable.ID,
                                                        dataPolicyId ),
                                                Require.exists(
                                                        DataPersistenceRule.class,
                                                        DataPersistenceRule.DATA_POLICY_ID,
                                                        Require.all(
                                                            Require.beanPropertyEquals(
                                                                    DataPersistenceRule.STATE,
                                                                    DataPlacementRuleState.INCLUSION_IN_PROGRESS ),
                                                            Require.beanPropertyEquals(
                                                                    DataPersistenceRule.STORAGE_DOMAIN_ID,
                                                                    storageDomainId ) ) ) ) ) ) ),
                /*
                 * Make sure it hasn't already been copied to this storage domain.
                 */
                Require.not( IomUtils.blobPersistedToStorageDomain(
                        storageDomainId, PersistenceProfile.NO_MOVING_OR_HEALING_REQUIRED ) ) );
    }


    public static < T extends DatabasePersistable & BlobTarget< ? >, S extends T > WhereClause blobTargetToBeIncluded(
            final Class< T > blobTargetType,
            final Class< S > suspectBlobTargetType,
            final UUID targetId,
            final UUID dataPolicyId )
    {
        return Require.all(
                /*
                 * Require that the target has a replication rule with inclusion in progress.
                 */
                Require.exists(
                        Blob.OBJECT_ID,
                        Require.exists(
                                S3Object.BUCKET_ID,
                                Require.exists(
                                        Bucket.DATA_POLICY_ID,
                                        Require.all(
                                                Require.beanPropertyEquals(
                                                        Identifiable.ID,
                                                        dataPolicyId ),
                                                Require.any(
                                                        Require.exists(
                                                                AzureDataReplicationRule.class,
                                                                AzureDataReplicationRule.DATA_POLICY_ID,
                                                                targetIsIncluding( targetId ) ),
                                                        Require.exists(
                                                                Ds3DataReplicationRule.class,
                                                                Ds3DataReplicationRule.DATA_POLICY_ID,
                                                                targetIsIncluding( targetId ) ),
                                                        Require.exists(
                                                                S3DataReplicationRule.class,
                                                                S3DataReplicationRule.DATA_POLICY_ID,
                                                                targetIsIncluding( targetId ) ) ) ) ) ) ),
                /*
                 * Make sure it hasn't already been copied to this target.
                 */
                Require.not( IomUtils.blobPersistedToTarget(
                        blobTargetType, suspectBlobTargetType, targetId, PersistenceProfile.NO_MOVING_OR_HEALING_REQUIRED ) ) );
    }


    public static WhereClause targetIsIncluding( final UUID targetId )
    {
        return Require.all(
                Require.beanPropertyEquals(
                        DataReplicationRule.STATE,
                        DataPlacementRuleState.INCLUSION_IN_PROGRESS ),
                Require.beanPropertyEquals(
                        DataReplicationRule.TARGET_ID,
                        targetId ) );
    }

       
    public static WhereClause tapeIsAutocompacting()
    {
        return Require.beanPropertyEquals(
                    Tape.STATE,
                    TapeState.AUTO_COMPACTION_IN_PROGRESS );
    }
    
    
    public static WhereClause tapeIsBad()
    {
        return Require.beanPropertyEqualsOneOf(
                Tape.STATE,
                BAD_TAPE_STATES );
    }
    
    
    private static WhereClause tapeOrPoolBelongsToStorageDomain(
            final UUID storageDomainId,
            final boolean includeMembersPendingExclusion )
    {
        final WhereClause sdmStateFilter;
        if ( includeMembersPendingExclusion )
        {
            sdmStateFilter = Require.nothing();
        }
        else
        {
            sdmStateFilter =
                    Require.beanPropertyEquals(
                            StorageDomainMember.STATE,
                            StorageDomainMemberState.NORMAL );
        }
        return Require.exists(
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                Require.all(
                    Require.beanPropertyEquals(
                            StorageDomainMember.STORAGE_DOMAIN_ID,
                            storageDomainId),
                    sdmStateFilter ) ) ;
    }
    
    
    public static WhereClause blobBelongsInBucket( final UUID bucketId )
    {
        return Require.exists(Blob.OBJECT_ID, Require.beanPropertyEquals(S3Object.BUCKET_ID, bucketId) );
    }
    
    
    public static WhereClause blobBelongsToDataPolicy( final UUID dataPolicyId )
    {
        return Require.exists( Blob.OBJECT_ID,
                Require.exists(
                        S3Object.BUCKET_ID,
                        Require.beanPropertyEquals(
                                Bucket.DATA_POLICY_ID,
                                dataPolicyId ) ) );
    }


    public static WhereClause tapeOrPoolPendingExclusion( final UUID storageDomainId )
    {
        return Require.exists(
                    PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                    Require.all(
                        Require.beanPropertyEquals(
                                StorageDomainMember.STORAGE_DOMAIN_ID,
                                storageDomainId),
                        Require.beanPropertyEquals(
                                StorageDomainMember.STATE,
                                StorageDomainMemberState.EXCLUSION_IN_PROGRESS ) ) ) ;
    }
    
     
    public static WhereClause blobNotPartOfAPutJob( final boolean includeIomJobs )
    {
        return Require.not( blobPartOfAPutJob( includeIomJobs ) );
    }
    
    
    public static WhereClause objectNotPartOfAPutJob( final boolean includeIomJobs )
    {
        return Require.not(
                Require.exists(
                        Blob.class,
                        Blob.OBJECT_ID,
                        blobPartOfAPutJob( includeIomJobs ) ) );
    }
    
    
    public static WhereClause blobPartOfAPutJob( final boolean includeIomJobs )
    {
        final WhereClause iomJobFilter;
        if ( includeIomJobs )
        {
            iomJobFilter = Require.nothing();
        }
        else
        {
            iomJobFilter =
                    Require.not( Require.exists( DataMigration.class, DataMigration.PUT_JOB_ID, Require.nothing() ) );
        }
        return Require.exists(
                        JobEntry.class,
                        BlobObservable.BLOB_ID,
                        Require.exists(
                                JobEntry.JOB_ID,
                                Require.all(
                                        iomJobFilter,
                                        Require.beanPropertyEquals(
                                                JobObservable.REQUEST_TYPE,
                                                JobRequestType.PUT ) ) ) );
    }
    
    
    private static Set< TapeState > getBadTapeStates()
    {
        final Set< TapeState > badStates = new HashSet<>();
        badStates.add( TapeState.BAD );
        return badStates;
    }
    
    private static final Set< TapeState > BAD_TAPE_STATES = getBadTapeStates();
}
