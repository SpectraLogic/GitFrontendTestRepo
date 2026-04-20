/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.persistencetarget;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.*;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.BlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.common.dao.orm.DataPolicyRM;
import com.spectralogic.s3.common.dao.orm.S3ObjectRM;
import com.spectralogic.s3.common.platform.iom.IomUtils;
import com.spectralogic.s3.common.platform.iom.PersistenceProfile;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.db.service.api.BeansServiceManager;

public final class PersistenceTargetUtil
{
    public static UUID getIsolatedBucketId( 
            final UUID bucketId, 
            final UUID storageDomainId,
            final BeansRetrieverManager brm )
    {
        return getIsolatedBucketId( 
                bucketId, storageDomainId, brm.getRetriever( DataPersistenceRule.class ) );
    }
    
    
    public static UUID getIsolatedBucketId( 
            final UUID bucketId, 
            final UUID storageDomainId,
            final BeansRetriever< DataPersistenceRule > ruleRetriever )
    {
        if (bucketId == null) {
			return null; // allows for this function to be called redundantly
		}
        final DataPersistenceRule rule = ruleRetriever.attain( Require.all( 
                Require.beanPropertyEquals( DataPersistenceRule.STORAGE_DOMAIN_ID, storageDomainId ),
                Require.exists( 
                        DataPlacement.DATA_POLICY_ID,
                        Require.exists(
                                Bucket.class, 
                                Bucket.DATA_POLICY_ID, 
                                Require.beanPropertyEquals( Identifiable.ID, bucketId ) ) ) ) );
        if ( DataIsolationLevel.STANDARD == rule.getIsolationLevel() )
        {
            return null;
        }
        return bucketId;
    }
    
    
    public static Set< BlobTape > findBlobTapesAvailableNow(
            final BeansRetriever< BlobTape > retriever,
            final Collection< UUID > blobIds,
            final boolean useUnavailableMedia,
            final Boolean searchOnEjectableMedia )
    {
        WhereClause storageDomainFilter = ( null == searchOnEjectableMedia ) ? null :Require.exists( 
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                Require.exists(
                        StorageDomainMember.STORAGE_DOMAIN_ID,
                        Require.beanPropertyEquals(
                                StorageDomain.MEDIA_EJECTION_ALLOWED, 
                                Boolean.valueOf( false ) ) ) );
        if ( null != searchOnEjectableMedia && searchOnEjectableMedia.booleanValue() )
        {
            storageDomainFilter = Require.not( storageDomainFilter );
        }
        
        return retriever.retrieveAll( Require.all(
                Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ),
                Require.exists( 
                        BlobTape.TAPE_ID,
                        Require.all( 
                                Require.any(
                                            Require.beanPropertyEquals(
                                            		Tape.STATE,
                                            		TapeState.NORMAL ),
                                            Require.beanPropertyEquals(
                                                    Tape.STATE,
                                                    TapeState.AUTO_COMPACTION_IN_PROGRESS ) ),
                                storageDomainFilter,
                                ( useUnavailableMedia ) ?
                                        null
                                        : Require.exists( Tape.PARTITION_ID, Require.all( 
                                            Require.beanPropertyEquals(
                                                    TapePartition.STATE, TapePartitionState.ONLINE ),
                                            Require.beanPropertyEquals(
                                                    TapePartition.QUIESCED, Quiesced.NO ) ) ) ) ),
                Require.not( Require.exists( SuspectBlobTape.class, Identifiable.ID, Require.nothing() ) ),
                Require.not( Require.exists( ObsoleteBlobTape.class, Identifiable.ID, Require.nothing() ) ) ) )
                .toSet();
    }


    public static Set< BlobPool > findBlobPoolsAvailableNow(
            final BeansRetriever< BlobPool > retriever,
            final Collection< UUID > blobIds,
            final PoolType poolType,
            final boolean useUnavailableMedia )
    {
        return retriever.retrieveAll( Require.all(
                Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ),
                Require.exists(
                        BlobPool.POOL_ID,
                        Require.all(
                                ( useUnavailableMedia ) ? 
                                        null
                                        : Require.beanPropertyEquals( Pool.QUIESCED, Quiesced.NO ),
                                Require.beanPropertyEquals( Pool.STATE, PoolState.NORMAL ),
                                ( null == poolType ) ?
                                        null
                                        : Require.beanPropertyEquals( PoolObservable.TYPE, poolType ) ) ),
                Require.not( Require.exists( SuspectBlobPool.class, Identifiable.ID, Require.nothing() ) ),
				Require.not( Require.exists( ObsoleteBlobPool.class, Identifiable.ID, Require.nothing() ) ) ) )
                .toSet();
    }
    
    
    public static WhereClause filterForWritableTapes(
            final UUID isolatedBucketId,
            final UUID storageDomainId,
            final long bytesToWrite,
            final Set< UUID > unavailableTapeIds,
            final UUID partitionId,
            final boolean unallocated )
    {
        // EMPROD-6309: Adding 1000MB check for sizing issue with  LT09 tapes
		final long minimumSize = Math.max(0, bytesToWrite - 1) + RESERVED_SPACE_ON_TAPE;
        return Require.all(
                filterForWritableTapes(),
                tapeAvailableForBucketAndDomain(
                        isolatedBucketId, storageDomainId, unavailableTapeIds, unallocated ),
                ( null == partitionId ) ? null : Require.beanPropertyEquals( Tape.PARTITION_ID, partitionId ),
                Require.beanPropertyGreaterThan(
                        Tape.AVAILABLE_RAW_CAPACITY,
                        minimumSize) );
    }


    public static WhereClause filterForWritableTapes(
            final UUID isolatedBucketId,
            final UUID storageDomainId )
    {
        return Require.all(
                filterForWritableTapes(),
                tapeAvailableForBucketAndDomain( isolatedBucketId, storageDomainId, null, false ) );
    }


    public static WhereClause filterForWritableTapes()
    {
        return Require.all(
                Require.beanPropertyEquals( Tape.STATE, TapeState.NORMAL ),
                Require.beanPropertyEquals( Tape.FULL_OF_DATA, Boolean.FALSE ),
                Require.beanPropertyEquals( Tape.WRITE_PROTECTED, Boolean.FALSE ),
                Require.beanPropertyEquals( Tape.EJECT_PENDING, null ),
                Require.beanPropertyEquals( Tape.ROLE, TapeRole.NORMAL),
                Require.exists(
                        Tape.PARTITION_ID,
                        Require.beanPropertyEquals( TapePartition.QUIESCED, Quiesced.NO) ) );
    }
    
    
    public static WhereClause filterForWritablePools(
            final UUID isolatedBucketId,
            final UUID storageDomainId,
            final long bytesToWrite,
            final Set< UUID > unavailablePoolIds,
            final boolean unallocated )
    {
		final long minimumSize = Math.max(0, bytesToWrite - 1); //this is an exclusive lower bound
        return Require.all( 
                filterForWritablePools(),
                poolAvailableForBucketAndDomain(
                        isolatedBucketId, storageDomainId, unavailablePoolIds, unallocated ),
                Require.beanPropertyGreaterThan( 
                        PoolObservable.AVAILABLE_CAPACITY, Long.valueOf( minimumSize ) ) );
    }
    
    
    public static WhereClause filterForWritablePools( 
            final UUID isolatedBucketId,
            final UUID storageDomainId )
    {
        return Require.all( 
                filterForWritablePools(),
                poolAvailableForBucketAndDomain( isolatedBucketId, storageDomainId, null, false ) );
    }

    
    public static WhereClause filterForWritablePools()
    {
        return Require.all(
                Require.beanPropertyEquals( Pool.STATE, PoolState.NORMAL ),
                Require.beanPropertyEquals( Pool.QUIESCED, Quiesced.NO ) );
    }
    

    private static WhereClause validMemberFilter(final UUID storageDomainId) {
        return Require.all(
                Require.beanPropertyEquals(
                        StorageDomainMember.STORAGE_DOMAIN_ID,
                        storageDomainId ),
                Require.not( Require.beanPropertyEquals(
                        StorageDomainMember.WRITE_PREFERENCE,
                        WritePreferenceLevel.NEVER_SELECT ) ),
                Require.beanPropertyEquals(
                        StorageDomainMember.STATE,
                        StorageDomainMemberState.NORMAL ) );
    }


    private static WhereClause poolAvailableForBucketAndDomain(
            final UUID isolatedBucketId,
            final UUID storageDomainId, 
            Set< UUID > unavailableIds,
            final boolean unallocated )
    {
        if ( null == unavailableIds )
        {
            unavailableIds = new HashSet<>();
        }
        return Require.all( 
                Require.not( Require.beanPropertyEqualsOneOf( Identifiable.ID, unavailableIds ) ),
                ( unallocated ) ?
                        Require.all(
                            Require.beanPropertyEquals(
                                    PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                                    Boolean.FALSE ),
                            ( null == storageDomainId ) ? Require.nothing()
                            : Require.exists(
                                    Pool.PARTITION_ID,
                                    Require.exists(
                                            StorageDomainMember.class,
                                            StorageDomainMember.POOL_PARTITION_ID,
                                            validMemberFilter(storageDomainId)
                                    )
                        ))
                        : ( null == storageDomainId ) ?
                                Require.beanPropertyEquals( 
                                        PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                                        Boolean.TRUE )
                                : Require.all(
                                        Require.exists(
                                                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                                                validMemberFilter(storageDomainId) ),
                                        Require.beanPropertyEquals( 
                                                PersistenceTarget.BUCKET_ID,
                                                isolatedBucketId ) ) );
    }


    private static WhereClause tapeAvailableForBucketAndDomain(
            final UUID isolatedBucketId,
            final UUID storageDomainId,
            Set< UUID > unavailableIds,
            final boolean unallocated )
    {
        if ( null == unavailableIds )
        {
            unavailableIds = new HashSet<>();
        }
        return Require.all(
                Require.not( Require.beanPropertyEqualsOneOf( Identifiable.ID, unavailableIds ) ),
                ( unallocated ) ?
                        Require.all(
                                Require.beanPropertyEquals(
                                        PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                                        Boolean.FALSE ),
                                ( null == storageDomainId ) ? Require.nothing()
                                : Require.exists(
                                        Tape.PARTITION_ID,
                                        Require.exists(
                                                StorageDomainMember.class,
                                                StorageDomainMember.TAPE_PARTITION_ID,
                                                validMemberFilter(storageDomainId)
                                        )
                                ))
                        : ( null == storageDomainId ) ?
                        Require.beanPropertyEquals(
                                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN,
                                Boolean.TRUE )
                        : Require.all(
                        Require.exists(
                                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                                validMemberFilter(storageDomainId) ),
                        Require.beanPropertyEquals(
                                PersistenceTarget.BUCKET_ID,
                                isolatedBucketId ) ) );
    }
    
    
    public static boolean isObjectFullyPersisted( final UUID objectId, final BeansServiceManager serviceManager )
    {
    	final DataPolicyRM dp = new S3ObjectRM( objectId, serviceManager ).getBucket().getDataPolicy();
    	for (final DataPersistenceRule rule : dp.getDataPersistenceRules().toSet() )
    	{
    		if (rule.getType().equals( DataPersistenceRuleType.PERMANENT ) )
    		{
	    		if ( serviceManager.getRetriever( Blob.class ).any(
	    				Require.all(
	    						Require.beanPropertyEquals( Blob.OBJECT_ID, objectId ),
	    						Require.not(
	    						IomUtils.blobPersistedToStorageDomain(
	    								rule.getStorageDomainId(),
	    								PersistenceProfile.DATA_INTEGRITY_OK_NOT_OBSOLETE ) ) ) ) )
				{
					return false;
				}
    		}
    	}
    	for ( final Ds3DataReplicationRule rule : dp.getDs3DataReplicationRules().toSet() )
    	{
    		if ( serviceManager.getRetriever( Blob.class ).any(
    				Require.all(
    						Require.beanPropertyEquals( Blob.OBJECT_ID, objectId ),
    						Require.not(
    						IomUtils.blobPersistedToTarget(
									BlobDs3Target.class,
									SuspectBlobDs3Target.class,
    								rule.getTargetId(),
    								PersistenceProfile.DATA_INTEGRITY_OK_NOT_OBSOLETE ) ) ) ) )
			{
				return false;
			}
    	}
    	for ( final AzureDataReplicationRule rule : dp.getAzureDataReplicationRules().toSet() )
    	{
    		if ( serviceManager.getRetriever( Blob.class ).any(
    				Require.all(
    						Require.beanPropertyEquals( Blob.OBJECT_ID, objectId ),
    						Require.not(
    						IomUtils.blobPersistedToTarget(
									BlobAzureTarget.class,
									SuspectBlobAzureTarget.class,
    								rule.getTargetId(),
    								PersistenceProfile.DATA_INTEGRITY_OK_NOT_OBSOLETE ) ) ) ) )
			{
				return false;
			}
    	}
    	for ( final S3DataReplicationRule rule : dp.getS3DataReplicationRules().toSet() )
    	{
    		if ( serviceManager.getRetriever( Blob.class ).any(
    				Require.all(
    						Require.beanPropertyEquals( Blob.OBJECT_ID, objectId ),
    						Require.not(
    						IomUtils.blobPersistedToTarget(
									BlobS3Target.class,
									SuspectBlobS3Target.class,
    								rule.getTargetId(),
    								PersistenceProfile.DATA_INTEGRITY_OK_NOT_OBSOLETE ) ) ) ) )
			{
				return false;
			}
    	}
    	return true;
    }

    //we reserve 100 MiB of extra space to prevent running out of space when reporting is slightly off
	public static final long RESERVED_SPACE_ON_TAPE = 100L * 1024L * 1024L;
}
