/*
 *
isStorageDomainInUse
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.frontend;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolPartition;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.domain.target.*;
import com.spectralogic.s3.common.dao.orm.DataPolicyRM;
import com.spectralogic.s3.common.dao.orm.PoolRM;
import com.spectralogic.s3.common.dao.orm.StorageDomainMemberRM;
import com.spectralogic.s3.common.dao.service.ds3.*;
import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.dao.service.tape.BlobTapeService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.dao.service.target.BlobAzureTargetService;
import com.spectralogic.s3.common.dao.service.target.BlobDs3TargetService;
import com.spectralogic.s3.common.dao.service.target.BlobS3TargetService;
import com.spectralogic.s3.common.dao.service.target.BlobTargetService;
import com.spectralogic.s3.common.rpc.dataplanner.DataPolicyManagementResource;
import com.spectralogic.s3.dataplanner.frmwk.DataPlannerException;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseRpcResource;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.net.rpc.server.RpcServer;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.util.*;

public final class DataPolicyManagementResourceImpl
        extends BaseRpcResource implements DataPolicyManagementResource
{
    public DataPolicyManagementResourceImpl(
            final RpcServer rpcServer,
            final BeansServiceManager serviceManager )
    {
        m_serviceManager = serviceManager;
        Validations.verifyNotNull( "Service manager", serviceManager );

        ensureBucketsInStandardIsolationLevelHaveSharedPersistenceTargetsAssignedOnly();
        rpcServer.register( null, this );
    }


    synchronized private void ensureBucketsInStandardIsolationLevelHaveSharedPersistenceTargetsAssignedOnly()
    {
        final Set< Tape > tapes = m_serviceManager.getRetriever( Tape.class ).retrieveAll(
                Require.not( Require.beanPropertyEquals( PersistenceTarget.BUCKET_ID, null ) ) ).toSet();
        final Set< Pool > pools = m_serviceManager.getRetriever( Pool.class ).retrieveAll(
                Require.not( Require.beanPropertyEquals( PersistenceTarget.BUCKET_ID, null ) ) ).toSet();
        final Set< UUID > bucketIds = new HashSet<>();
        bucketIds.addAll( BeanUtils.extractPropertyValues( tapes, PersistenceTarget.BUCKET_ID ) );
        bucketIds.addAll( BeanUtils.extractPropertyValues( pools, PersistenceTarget.BUCKET_ID ) );
        final Map< UUID, Bucket > buckets = BeanUtils.toMap(
                m_serviceManager.getRetriever( Bucket.class ).retrieveAll( bucketIds ).toSet() );

        final Map< UUID, Map< UUID, DataPersistenceRule > > persistenceRules = new HashMap<>();
        for ( final DataPersistenceRule rule
                : m_serviceManager.getRetriever( DataPersistenceRule.class ).retrieveAll().toSet() )
        {
            if ( !persistenceRules.containsKey( rule.getDataPolicyId() ) )
            {
                persistenceRules.put( rule.getDataPolicyId(), new HashMap<>() );
            }
            persistenceRules.get( rule.getDataPolicyId() ).put( rule.getStorageDomainId(), rule );
        }

        for ( final Tape tape : getPersistenceTargetsThatShouldBeShared( tapes, buckets, persistenceRules ) )
        {
            m_serviceManager.getService( TapeService.class ).update(
                    tape.setBucketId( null ), PersistenceTarget.BUCKET_ID );
        }
        for ( final Pool pool : getPersistenceTargetsThatShouldBeShared( pools, buckets, persistenceRules ) )
        {
            m_serviceManager.getService( PoolService.class ).update(
                    pool.setBucketId( null ), PersistenceTarget.BUCKET_ID );
        }
    }


    private < T extends PersistenceTarget< T > > Set< T > getPersistenceTargetsThatShouldBeShared(
            final Set< T > persistenceTargets,
            final Map< UUID, Bucket > buckets,
            final Map< UUID, Map< UUID, DataPersistenceRule > > persistenceRules )
    {
        final Set< T > retval = new HashSet<>();
        for ( final T pt : persistenceTargets )
        {
            final Bucket bucket = buckets.get( pt.getBucketId() );
            final Map< UUID, DataPersistenceRule > rules = persistenceRules.get( bucket.getDataPolicyId() );
            if ( null != pt.getStorageDomainMemberId() )
            {
                final DataPersistenceRule rule = rules.get(new StorageDomainMemberRM(
                        pt.getStorageDomainMemberId(), m_serviceManager).unwrap().getStorageDomainId());
                if (DataIsolationLevel.STANDARD == rule.getIsolationLevel())
                {
                    retval.add(pt);
                }
            }
        }

        return retval;
    }


    @Override
    synchronized public RpcFuture< UUID > createBucket( final Bucket bucket )
    {
        if ( getStorageDomainsTargeted(
                bucket.getDataPolicyId(),
                DataPersistenceRuleType.PERMANENT,
                TargetHealth.INCLUDE_ALL_TARGETS ).isEmpty() &&
                getReplicationTargets(
                        bucket.getDataPolicyId(),
                        DataReplicationRuleType.PERMANENT,
                        TargetHealth.INCLUDE_ALL_TARGETS ).isEmpty() )
        {
            final DataPolicy dataPolicy =
                    m_serviceManager.getRetriever( DataPolicy.class ).attain( bucket.getDataPolicyId() );
            final String dataPolicyName = dataPolicy.getName();
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "Data policy " + dataPolicyName
                            + " doesn't have at least one permanent persistence or replication rule." );
        }

        m_serviceManager.getService( BucketService.class ).create( bucket );
        return new RpcResponse<>( bucket.getId() );
    }


    @Override
    synchronized public RpcFuture< ? > modifyBucket(
            final UUID bucketId,
            final UUID dataPolicyId )
    {
        final Bucket originalBucket =
                m_serviceManager.getRetriever( Bucket.class ).attain( bucketId );
        final DataPolicy originalDataPolicy =
                m_serviceManager.getRetriever( DataPolicy.class ).attain( originalBucket.getDataPolicyId() );
        final DataPolicy newDataPolicy =
                m_serviceManager.getRetriever( DataPolicy.class ).attain( dataPolicyId );
        verifyDataPoliciesCompatibleAndMigrate( originalBucket, newDataPolicy, originalDataPolicy );

        return null;
    }


    private void verifyDataPoliciesCompatibleAndMigrate(
            final Bucket bucket,
            final DataPolicy policyDest,
            final DataPolicy policySrc )
    {
        for ( final String prop : BeanUtils.getPropertyNames( DataPolicy.class ) )
        {
            if ( DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.contains( prop )
                    || Identifiable.ID.equals( prop ) )
            {
                continue;
            }

            final Object value1;
            final Object value2;
            final Method reader = BeanUtils.getReader( DataPolicy.class, prop );
            try
            {
                value1 = reader.invoke( policySrc );
                value2 = reader.invoke( policyDest );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( "Failed to invoke " + reader + ".", ex );
            }

            if ( !value1.equals( value2 ) )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Data policy property " + prop + " is incompatible: " + value1
                                + " is not compatible with " + value2 + "." );
            }
        }

        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            migrateBucketToNewDataPolicyForDataPersistenceRules(
                    transaction,
                    bucket,
                    policySrc,
                    policyDest );
            migrateBucketToNewDataPolicyForDataReplicationRules(
                    transaction,
                    Ds3DataReplicationRule.class,
                    Ds3Target.class,
                    DegradedBlob.DS3_REPLICATION_RULE_ID,
                    bucket,
                    policySrc,
                    policyDest );
            migrateBucketToNewDataPolicyForDataReplicationRules(
                    transaction,
                    S3DataReplicationRule.class,
                    S3Target.class,
                    DegradedBlob.S3_REPLICATION_RULE_ID,
                    bucket,
                    policySrc,
                    policyDest );
            migrateBucketToNewDataPolicyForDataReplicationRules(
                    transaction,
                    AzureDataReplicationRule.class,
                    AzureTarget.class,
                    DegradedBlob.AZURE_REPLICATION_RULE_ID,
                    bucket,
                    policySrc,
                    policyDest );

            transaction.getService( BucketService.class ).update(
                    bucket.setDataPolicyId( policyDest.getId() ),
                    Bucket.DATA_POLICY_ID );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }


    private void migrateBucketToNewDataPolicyForDataPersistenceRules(
            final BeansServiceManager transaction,
            final Bucket bucket,
            final DataPolicy policySrc,
            final DataPolicy policyDest )
    {
        for ( final DataPersistenceRuleType type : DataPersistenceRuleType.values() )
        {
            final Map< UUID, DataIsolationLevel > storageDomains1 = getStorageDomainsTargeted(
                    policySrc.getId(), type, TargetHealth.INCLUDE_ALL_TARGETS );
            final Map< UUID, DataIsolationLevel > storageDomains2 = getStorageDomainsTargeted(
                    policyDest.getId(), type, TargetHealth.INCLUDE_ALL_TARGETS );
            if ( !storageDomains1.keySet().equals( storageDomains2.keySet() ) )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "The storage domains targeted via " + type
                                + " persistence rules don't match between data policy " + policySrc.getName()
                                + " and " + policyDest.getName() + "." );
            }
            if ( !storageDomains1.equals( storageDomains2 ) )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "The data isolation policies for the " + type
                                + " persistence rules don't match between data policy " + policySrc.getName()
                                + " and " + policyDest.getName() + "." );
            }
            for ( final UUID storageDomainId : storageDomains1.keySet() )
            {
                final DataPersistenceRule ruleSrc =
                        transaction.getRetriever( DataPersistenceRule.class ).attain( Require.all(
                                Require.beanPropertyEquals(
                                        DataPlacement.DATA_POLICY_ID, policySrc.getId() ),
                                Require.beanPropertyEquals(
                                        DataPersistenceRule.STORAGE_DOMAIN_ID, storageDomainId ) ) );
                final DataPersistenceRule ruleDest =
                        transaction.getRetriever( DataPersistenceRule.class ).attain( Require.all(
                                Require.beanPropertyEquals(
                                        DataPlacement.DATA_POLICY_ID, policyDest.getId() ),
                                Require.beanPropertyEquals(
                                        DataPersistenceRule.STORAGE_DOMAIN_ID, storageDomainId ) ) );
                if ( DataPlacementRuleState.INCLUSION_IN_PROGRESS == ruleSrc.getState() )
                {
                    transaction.getUpdater( DataPersistenceRule.class ).update(
                            ruleDest.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS),
                            DataPlacement.STATE );
                }
                transaction.getService( DegradedBlobService.class ).migrate(
                        DegradedBlob.PERSISTENCE_RULE_ID,
                        bucket.getId(),
                        ruleDest.getId(),
                        ruleSrc.getId() );
            }
        }
    }


    private < T extends DatabasePersistable & DataReplicationRule< T > >
    void migrateBucketToNewDataPolicyForDataReplicationRules(
            final BeansServiceManager transaction,
            final Class< T > replicationRuleType,
            final Class< ? extends DatabasePersistable > targetType,
            final String degradedBlobRuleProperty,
            final Bucket bucket,
            final DataPolicy policySrc,
            final DataPolicy policyDest )
    {
        for ( final DataReplicationRuleType type : DataReplicationRuleType.values() )
        {
            final Set< UUID > targets1 = BeanUtils.toMap(
                    transaction.getRetriever( targetType ).retrieveAll( Require.all( Require.exists(
                            replicationRuleType,
                            DataReplicationRule.TARGET_ID,
                            Require.beanPropertyEquals(
                                    DataPlacement.DATA_POLICY_ID,
                                    policySrc.getId() ) ) ) ).toSet() ).keySet();
            final Set< UUID > targets2 = BeanUtils.toMap(
                    transaction.getRetriever( targetType ).retrieveAll( Require.all( Require.exists(
                            replicationRuleType,
                            DataReplicationRule.TARGET_ID,
                            Require.beanPropertyEquals(
                                    DataPlacement.DATA_POLICY_ID,
                                    policyDest.getId() ) ) ) ).toSet() ).keySet();
            if ( !targets1.equals( targets2 ) )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "The " + targetType.getSimpleName() + "s targeted via " + type
                                + " replication rules don't match between data policy " + policySrc.getName()
                                + " and " + policyDest.getName() + "." );
            }
            for ( final UUID targetId : targets1 )
            {
                final T ruleSrc = transaction.getRetriever( replicationRuleType ).attain( Require.all(
                        Require.beanPropertyEquals(
                                DataPlacement.DATA_POLICY_ID, policySrc.getId() ),
                        Require.beanPropertyEquals(
                                DataReplicationRule.TARGET_ID, targetId ) ) );
                final T ruleDest = transaction.getRetriever( replicationRuleType ).attain( Require.all(
                        Require.beanPropertyEquals(
                                DataPlacement.DATA_POLICY_ID, policyDest.getId() ),
                        Require.beanPropertyEquals(
                                DataReplicationRule.TARGET_ID, targetId ) ) );
                if ( DataPlacementRuleState.INCLUSION_IN_PROGRESS == ruleSrc.getState() )
                {
                    LOG.info( "Source replication rule " + ruleSrc.getId()
                            + " is in inclusion in progress state, setting destination rule "
                            + ruleDest.getId() + " to inclusion in progress state." );
                    transaction.getUpdater( replicationRuleType ).update(
                            ruleDest.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS),
                            DataPlacement.STATE );
                }
                transaction.getService( DegradedBlobService.class ).migrate(
                        degradedBlobRuleProperty,
                        bucket.getId(),
                        ruleDest.getId(),
                        ruleSrc.getId() );
            }
        }
    }


    private enum TargetHealth
    {
        INCLUDE_HEALTHY_TARGETS_ONLY,
        INCLUDE_ALL_TARGETS,
        INCLUDE_INCLUSION_IN_PROGRESS_TARGETS,
    }


    private Map< UUID, DataIsolationLevel > getStorageDomainsTargeted(
            final UUID dataPolicyId,
            final DataPersistenceRuleType type,
            final TargetHealth targetHealthRequirement )
    {
        final Set< DataPersistenceRule > rules =
                m_serviceManager.getRetriever( DataPersistenceRule.class ).retrieveAll( Require.all(
                        Require.beanPropertyEquals( DataPlacement.DATA_POLICY_ID, dataPolicyId ),
                        Require.beanPropertyEquals( DataPersistenceRule.TYPE, type ) ) ).toSet();

        Set< DataPersistenceRule > unhealthyRules = null;
        switch (targetHealthRequirement)
        {
            case INCLUDE_ALL_TARGETS:
                // empty set
                unhealthyRules = new HashSet< >();
                break;
            case INCLUDE_HEALTHY_TARGETS_ONLY:
                unhealthyRules = m_serviceManager.getRetriever( DataPersistenceRule.class ).retrieveAll( Require.all(
                        Require.beanPropertyEqualsOneOf(
                                Identifiable.ID,
                                BeanUtils.extractPropertyValues( rules, Identifiable.ID ) ),
                        Require.any(
                                Require.exists(
                                        DegradedBlob.class,
                                        DegradedBlob.PERSISTENCE_RULE_ID,
                                        Require.nothing() ),
                                Require.beanPropertyEquals(
                                        DataPlacement.STATE,
                                        DataPlacementRuleState.INCLUSION_IN_PROGRESS )
                        ) ) ).toSet();
                break;
            case INCLUDE_INCLUSION_IN_PROGRESS_TARGETS:
                unhealthyRules = m_serviceManager.getRetriever( DataPersistenceRule.class ).retrieveAll( Require.all(
                        Require.beanPropertyEqualsOneOf(
                                Identifiable.ID,
                                BeanUtils.extractPropertyValues( rules, Identifiable.ID ) ),
                        Require.any(
                                Require.exists(
                                        DegradedBlob.class,
                                        DegradedBlob.PERSISTENCE_RULE_ID,
                                        Require.nothing() )
                        ) ) ).toSet();
                break;
        }

        final Set< UUID > unhealthyRuleIds =
                BeanUtils.extractPropertyValues( unhealthyRules, Identifiable.ID );

        final Map< UUID, DataIsolationLevel > retval = new HashMap<>();
        for ( final DataPersistenceRule rule : new HashSet<>( rules ) )
        {
            if ( !unhealthyRuleIds.contains( rule.getId() ) )
            {
                retval.put( rule.getStorageDomainId(), rule.getIsolationLevel() );
            }
        }
        LOG.info( retval.size() + " storage domains are targeted by data policy " + dataPolicyId + " via "
                + type + " persistence rules"
                + ( ( TargetHealth.INCLUDE_HEALTHY_TARGETS_ONLY == targetHealthRequirement ) ?
                ".  " + unhealthyRules.size() + " targets were unacceptable due to blob degradation or"
                        + " inclusion that is not yet complete."
                : "" )
                + "." );

        return retval;
    }


    private Map< UUID, DataPlacementRuleState > getReplicationTargets(
            final UUID dataPolicyId,
            final DataReplicationRuleType type,
            TargetHealth healthRule)
    {
        WhereClause stateFilter = Require.nothing();
        WhereClause azureFilter = Require.nothing();
        WhereClause s3Filter = Require.nothing();
        WhereClause ds3Filter = Require.nothing();

        if (healthRule == TargetHealth.INCLUDE_HEALTHY_TARGETS_ONLY) {
            stateFilter = Require.beanPropertyEquals( DataPlacement.STATE, DataPlacementRuleState.NORMAL );
        }

        if ( healthRule != TargetHealth.INCLUDE_ALL_TARGETS ) {
            azureFilter = Require.not(
                    Require.exists(
                            DegradedBlob.class,
                            DegradedBlob.AZURE_REPLICATION_RULE_ID,
                            Require.nothing()));
            s3Filter = Require.not(
                    Require.exists(
                            DegradedBlob.class,
                            DegradedBlob.S3_REPLICATION_RULE_ID,
                            Require.nothing()));
            ds3Filter = Require.not(
                    Require.exists(
                            DegradedBlob.class,
                            DegradedBlob.DS3_REPLICATION_RULE_ID,
                            Require.nothing()));
        }

        final Set< AzureDataReplicationRule > azureRules =
                m_serviceManager.getRetriever( AzureDataReplicationRule.class ).retrieveAll( Require.all(
                        Require.beanPropertyEquals( DataPlacement.DATA_POLICY_ID, dataPolicyId ),
                        Require.beanPropertyEquals( DataReplicationRule.TYPE, type ),
                        stateFilter,
                        azureFilter ) ).toSet();
        final Set< S3DataReplicationRule > s3Rules =
                m_serviceManager.getRetriever( S3DataReplicationRule.class ).retrieveAll( Require.all(
                        Require.beanPropertyEquals( DataPlacement.DATA_POLICY_ID, dataPolicyId ),
                        Require.beanPropertyEquals( DataReplicationRule.TYPE, type ),
                        stateFilter,
                        s3Filter ) ).toSet();
        final Set< Ds3DataReplicationRule > ds3Rules =
                m_serviceManager.getRetriever( Ds3DataReplicationRule.class ).retrieveAll( Require.all(
                        Require.beanPropertyEquals( DataPlacement.DATA_POLICY_ID, dataPolicyId ),
                        Require.beanPropertyEquals( DataReplicationRule.TYPE, type ),
                        stateFilter,
                        ds3Filter ) ).toSet();
        Set< DataReplicationRule > rules = new HashSet<>();
        for ( final DataReplicationRule rule : new HashSet<>( azureRules ) )
        {
            rules.add( rule );
        }
        for ( final DataReplicationRule rule : new HashSet<>( ds3Rules ) )
        {
            rules.add( rule );
        }
        for ( final DataReplicationRule rule : new HashSet<>( s3Rules ) )
        {
            rules.add( rule );
        }

        final Map< UUID, DataPlacementRuleState > retval = new HashMap<>();
        for ( final DataReplicationRule rule : new HashSet<>( rules ) )
        {
            retval.put( rule.getTargetId(), rule.getState() );
        }
        LOG.info( retval.size() + " replication targets are targeted by data policy " + dataPolicyId + " via "
                + type + " replication rules.");

        return retval;
    }


    @Override
    synchronized public RpcFuture< ? > modifyDataPolicy(
            final DataPolicy dataPolicy,
            final String [] arrayPropertiesToUpdate )
    {
        final Set< String > propertiesToUpdate = CollectionFactory.toSet( arrayPropertiesToUpdate );
        if ( isDataPolicyInUse( dataPolicy.getId() ) )
        {
            for ( final String prop : propertiesToUpdate )
            {
                if ( !DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.contains( prop )
                        && !DataPolicy.VERSIONING.equals( prop ) )
                {
                    throw new DataPlannerException(
                            GenericFailure.CONFLICT,
                            "Cannot modify property " + prop
                                    + " of data policy since the data policy is being used by at least one bucket." );
                }
            }
        }

        if ( propertiesToUpdate.contains( DataPolicy.MAX_VERSIONS_TO_KEEP ) )
        {
            if ( 0 >= dataPolicy.getMaxVersionsToKeep() )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Number of versions to keep must be greater than 0." );
            }
        }

        if ( propertiesToUpdate.contains( DataPolicy.DEFAULT_BLOB_SIZE ) )
        {
            if ( null != dataPolicy.getDefaultBlobSize() && 0 >= dataPolicy.getDefaultBlobSize() )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Default blob size must be greater than 0." );
            }
        }

        if ( propertiesToUpdate.contains( DataPolicy.VERSIONING ) )
        {
            verifyVersioningNotDowngraded( dataPolicy.getId(), dataPolicy.getVersioning() );
            if ( !dataPolicy.getVersioning().isLtfsObjectNamingAllowed() )
            {
                if ( 0 < m_serviceManager.getRetriever( StorageDomain.class ).getCount(
                        Require.all(
                                Require.beanPropertyEquals(
                                        StorageDomain.LTFS_FILE_NAMING,
                                        LtfsFileNamingMode.OBJECT_NAME ),
                                Require.exists(
                                        DataPersistenceRule.class,
                                        DataPersistenceRule.STORAGE_DOMAIN_ID,
                                        Require.beanPropertyEquals(
                                                DataPlacement.DATA_POLICY_ID,
                                                dataPolicy.getId() ) ) ) ) )
                {
                    throw new DataPlannerException(
                            GenericFailure.CONFLICT,
                            "Cannot modify property " + DataPolicy.VERSIONING
                                    + " of data policy since the data policy targets one or more "
                                    + "storage domains using the "
                                    + LtfsFileNamingMode.OBJECT_NAME + " "
                                    + StorageDomain.LTFS_FILE_NAMING + "." );
                }
            }
        }

        m_serviceManager.getService( DataPolicyService.class ).update(
                dataPolicy,
                CollectionFactory.toArray( String.class, propertiesToUpdate ) );
        return null;
    }


    private void verifyVersioningNotDowngraded( final UUID dataPolicyId, final VersioningLevel newValue )
    {
        final DataPolicy original = m_serviceManager.getRetriever( DataPolicy.class ).attain( dataPolicyId );
        if ( original.getVersioning().ordinal() > newValue.ordinal() && isDataPolicyInUse( dataPolicyId ) )
        {
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "Cannot downgrade " + DataPolicy.VERSIONING + " from " + original.getVersioning()
                            + " to " + newValue + " since the data policy is being used by at least one bucket." );
        }
    }


    private boolean isDataPolicyInUse( final UUID dataPolicyId )
    {
        return ( 0 < m_serviceManager.getRetriever( Bucket.class ).getCount(
                Bucket.DATA_POLICY_ID, dataPolicyId ) );
    }


    private void verifyDataPolicyNotInUse( final UUID dataPolicyId )
    {
        if ( isDataPolicyInUse( dataPolicyId ) )
        {
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "Cannot perform the requested operation since the data policy "
                            + "is being used by at least one bucket." );
        }
    }


    @Override
    synchronized public RpcFuture< UUID > createDataPersistenceRule( final DataPersistenceRule rule )
    {
        try ( final NestableTransaction transaction = m_serviceManager.startNestableTransaction() )
        {
            if ( DataPersistenceRuleType.RETIRED == rule.getType() )
            {
                throw new DataPlannerException(
                        GenericFailure.BAD_REQUEST,
                        "Only existing persistence rules can be retired.  "
                                + "It is non-sensical to create a retired persistence rule." );
            }
            if ( DataPersistenceRuleType.PERMANENT == rule.getType() )
            {
                if ( isDataPolicyInUse( rule.getDataPolicyId() ) )
                {
                    if ( transaction.getRetriever( DataPathBackend.class ).attain( Require.nothing() ).isIomEnabled() )
                    {
                        if ( allowedToCreateRule( rule, transaction ) )
                        {
                            rule.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS );
                        }
                        else
                        {
                            throw new DataPlannerException(
                                    GenericFailure.CONFLICT,
                                    "There are outstanding PUT jobs for buckets that would be affected by this change. "
                                            + "Please allow these jobs to complete, or cancel them prior to adding a new "
                                            + "data persistence rule." );
                        }
                    }
                    else
                    {
                        throw new DataPlannerException(
                                GenericFailure.CONFLICT,
                                "Cannot perform the requested operation since the data policy "
                                        + "is being used by at least one bucket and IOM is not enabled." );
                    }
                }
            }
            else if ( DataPersistenceRuleType.TEMPORARY == rule.getType() )
            {
                verifyStorageDomainContainsNoTape( rule.getStorageDomainId() );
                verifyMinDaysToRetainNotTooShort( rule.getMinimumDaysToRetain(), rule.getStorageDomainId() );
            }
            if ( getStorageDomainMembers( rule.getStorageDomainId(), true, transaction ).isEmpty() )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Cannot perform the requested operation since the storage domain "
                                + "doesn't have any writable members." );
            }


            final DataPolicyService dataPolicyService = transaction.getService( DataPolicyService.class );
            final DataPolicy dataPolicy = dataPolicyService.attain( rule.getDataPolicyId() );
            final StorageDomain storageDomain =
                    transaction.getRetriever( StorageDomain.class ).attain( rule.getStorageDomainId() );
            if ( VersioningLevel.NONE != dataPolicy.getVersioning() )
            {
                if ( LtfsFileNamingMode.OBJECT_NAME == storageDomain.getLtfsFileNaming() )
                {
                    throw new DataPlannerException(
                            GenericFailure.CONFLICT,
                            "Data policy cannot target storage domains with "
                                    + storageDomain.getLtfsFileNaming()
                                    + " "
                                    + StorageDomain.LTFS_FILE_NAMING
                                    + " since data policy versioning level is "
                                    + dataPolicy.getVersioning() + "." );
                }
            }
            if ( !dataPolicyService.areStorageDomainsWithObjectNamingAllowed( dataPolicy ) )
            {
                if ( LtfsFileNamingMode.OBJECT_NAME == storageDomain.getLtfsFileNaming() )
                {
                    throw new DataPlannerException(
                            GenericFailure.CONFLICT,
                            "Data policy cannot target storage domains with "
                                    + storageDomain.getLtfsFileNaming()
                                    + " "
                                    + StorageDomain.LTFS_FILE_NAMING
                                    + " since objects were created that are not compatible with that "
                                    + StorageDomain.LTFS_FILE_NAMING + "." );
                }
            }

            transaction.getService( DataPersistenceRuleService.class ).create( rule );
            transaction.commitNestableTransaction();
            return new RpcResponse<>( rule.getId() );
        }
    }


    private static Set< StorageDomainMember > getStorageDomainMembers(
            final UUID storageDomainId,
            final boolean includePool,
            final BeansServiceManager bsm )
    {
        return bsm.getRetriever( StorageDomainMember.class ).retrieveAll( Require.all(
                Require.beanPropertyEquals( StorageDomainMember.STORAGE_DOMAIN_ID, storageDomainId ),
                Require.all(
                        Require.any(
                                Require.beanPropertyEquals( StorageDomainMember.POOL_PARTITION_ID, null ),
                                ( includePool ) ?
                                        Require.beanPropertyEquals(
                                                StorageDomainMember.TAPE_PARTITION_ID, null )
                                        : null ),
                        Require.beanPropertyEquals(
                                StorageDomainMember.STATE,
                                StorageDomainMemberState.NORMAL ),
                        Require.not( Require.beanPropertyEquals(
                                StorageDomainMember.WRITE_PREFERENCE,
                                WritePreferenceLevel.NEVER_SELECT ) ) ) ) ).toSet();
    }


    private void verifyStorageDomainContainsNoTape( final UUID storageDomainId )
    {
        if ( !getStorageDomainMembers( storageDomainId, false, m_serviceManager ).isEmpty() )
        {
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "Tape cannot be used as a temporary persistence target." );
        }
    }


    @Override
    synchronized public RpcFuture< ? > modifyDataPersistenceRule(
            final DataPersistenceRule rule,
            final String [] arrayPropertiesToModify )
    {
        final DataPersistenceRule original =
                m_serviceManager.getRetriever( DataPersistenceRule.class ).attain( rule.getId() );
        final Set< String > propertiesToModify = CollectionFactory.toSet( arrayPropertiesToModify );
        final DataPersistenceRuleType type = ( propertiesToModify.contains( DataPersistenceRule.TYPE ) ) ?
                rule.getType()
                : original.getType();
        if ( propertiesToModify.contains( DataPersistenceRule.ISOLATION_LEVEL ) )
        {
            final boolean wasStandard =
                    ( DataIsolationLevel.STANDARD == original.getIsolationLevel() );
            final boolean newlyStandard =
                    ( DataIsolationLevel.STANDARD == rule.getIsolationLevel() );
            if ( wasStandard && !newlyStandard )
            {
                if ( 0 < m_serviceManager.getRetriever( Bucket.class ).getCount(
                        Bucket.DATA_POLICY_ID, rule.getDataPolicyId() ) )
                {
                    throw new DataPlannerException(
                            GenericFailure.CONFLICT,
                            "Cannot upgrade the " + DataPersistenceRule.ISOLATION_LEVEL
                                    + " since the data policy is being used by at least one bucket." );
                }
            }
            if ( newlyStandard && !wasStandard )
            {
                propertiesToModify.remove( DataPersistenceRule.ISOLATION_LEVEL );
                m_serviceManager.getService( DataPersistenceRuleService.class ).update(
                        rule, DataPersistenceRule.ISOLATION_LEVEL );
                ensureBucketsInStandardIsolationLevelHaveSharedPersistenceTargetsAssignedOnly();
            }
        }
        if ( propertiesToModify.contains( DataPersistenceRule.TYPE ) )
        {
            if ( DataPersistenceRuleType.PERMANENT == rule.getType() )
            {
                verifyDataPolicyNotInUse( rule.getDataPolicyId() );
            }
            if ( DataPersistenceRuleType.TEMPORARY == rule.getType() )
            {
                verifyStorageDomainContainsNoTape( rule.getStorageDomainId() );
                verifyMinDaysToRetainNotTooShort(
                        rule.getMinimumDaysToRetain(),
                        rule.getStorageDomainId() );
            }
            if ( DataPersistenceRuleType.TEMPORARY == rule.getType() && DataPersistenceRuleType.RETIRED == original.getType() )
            {
                verifyHealthyRuleExists( rule );
            }
            else
            {
                TargetHealth healthRule = TargetHealth.INCLUDE_HEALTHY_TARGETS_ONLY;
                if ( DataPersistenceRuleType.RETIRED == rule.getType() &&  DataPersistenceRuleType.PERMANENT == original.getType() )
                    healthRule = TargetHealth.INCLUDE_INCLUSION_IN_PROGRESS_TARGETS;
                verifyNotFinalHealthyPermPersistenceIfInUse( rule, healthRule );
            }
        }
        if ( propertiesToModify.contains( DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN )
                && DataPersistenceRuleType.TEMPORARY == type )
        {
            verifyMinDaysToRetainNotTooShort(
                    rule.getMinimumDaysToRetain(),
                    rule.getStorageDomainId() );
        }

        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            transaction.getService( DataPersistenceRuleService.class ).update(
                    rule, CollectionFactory.toArray( String.class, propertiesToModify ) );
            if ( DataPersistenceRuleType.PERMANENT == original.getType()
                    && DataPersistenceRuleType.PERMANENT != rule.getType() )
            {
                transaction.getService( DegradedBlobService.class ).deleteAllForPersistenceRule(
                        rule.getId() );
            }
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        return null;
    }

    // Input rule is not Permanent, so we don't need to cross-check it. But we want to fail the original request
    // if there is not at least one healthy target.
    private void verifyHealthyRuleExists( final DataPersistenceRule rule )
    {
        final Map< UUID, DataIsolationLevel > healthyStorageDomains = getStorageDomainsTargeted(
                rule.getDataPolicyId(),
                DataPersistenceRuleType.PERMANENT,
                TargetHealth.INCLUDE_HEALTHY_TARGETS_ONLY );
        if ( healthyStorageDomains.isEmpty() )
        {
            // TODO: error message that results is generic and confusing. Would be nice to have a more specific message.
            verifyDataPolicyNotInUse( rule.getDataPolicyId() );
        }
    }

    private void verifyNotFinalHealthyPermPersistenceIfInUse(final DataPersistenceRule rule, TargetHealth healthRule )
    {
        final Map< UUID, DataIsolationLevel > storageDomainsTargeted = getStorageDomainsTargeted(
                rule.getDataPolicyId(),
                DataPersistenceRuleType.PERMANENT,
                healthRule );
        final Map<UUID, DataPlacementRuleState> replicationTargetsTargeted = getReplicationTargets(rule.getDataPolicyId(),
                DataReplicationRuleType.PERMANENT,
                healthRule);
        final int totalCopies = storageDomainsTargeted.size() + replicationTargetsTargeted.size();
        if ( totalCopies == 0 || ( totalCopies == 1 && storageDomainsTargeted.keySet().contains( rule.getStorageDomainId() ) ) )
        {
            verifyDataPolicyNotInUse( rule.getDataPolicyId() );
        }
    }


    private void verifyNotFinalHealthyPermReplicationIfInUse(final DataReplicationRule rule, TargetHealth healthRule )
    {
        final Map< UUID, DataIsolationLevel > storageDomainsTargeted = getStorageDomainsTargeted(
                rule.getDataPolicyId(),
                DataPersistenceRuleType.PERMANENT,
                healthRule );
        final Map<UUID, DataPlacementRuleState> replicationTargetsTargeted = getReplicationTargets(rule.getDataPolicyId(),
                DataReplicationRuleType.PERMANENT,
                healthRule);
        final int totalCopies = storageDomainsTargeted.size() + replicationTargetsTargeted.size();
        if ( totalCopies == 0 || ( totalCopies == 1 && replicationTargetsTargeted.keySet().contains( rule.getTargetId() ) ) )
        {
            verifyDataPolicyNotInUse( rule.getDataPolicyId() );
        }
    }


    private void verifyMinDaysToRetainNotTooShort( final Integer minDaysToRetain, final UUID storageDomainId )
    {
        if ( null == minDaysToRetain )
        {
            throw new DataPlannerException(
                    GenericFailure.BAD_REQUEST,
                    "Temporary persistence rules must specify a value for "
                            + DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN + "." );
        }
        if ( minDaysToRetain >= MIN_MIN_DAYS_TO_RETAIN_FOR_NEARLINE_POOL )
        {
            return;
        }

        final Set< PoolPartition > nearlinePoolPartitions =
                m_serviceManager.getRetriever( PoolPartition.class ).retrieveAll( Require.all(
                        Require.beanPropertyEquals( PoolPartition.TYPE, PoolType.NEARLINE ),
                        Require.exists(
                                StorageDomainMember.class,
                                StorageDomainMember.POOL_PARTITION_ID,
                                Require.beanPropertyEquals(
                                        StorageDomainMember.STORAGE_DOMAIN_ID,
                                        storageDomainId ) ) ) ).toSet();
        if ( nearlinePoolPartitions.isEmpty() )
        {
            return;
        }

        throw new DataPlannerException(
                GenericFailure.CONFLICT,
                "Since the data persistence rule targets a storage domain that contains " + PoolType.NEARLINE
                        + " pool storage, the " + DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN + " must be at least "
                        + MIN_MIN_DAYS_TO_RETAIN_FOR_NEARLINE_POOL + "." );
    }


    @Override
    synchronized public RpcFuture< ? > deleteDataPersistenceRule( final UUID ruleId )
    {
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            final DataPersistenceRule rule =
                    transaction.getRetriever( DataPersistenceRule.class ).attain( ruleId );
            deleteDataPersistenceRuleInternal( transaction, rule );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        return null;
    }


    private void deleteDataPersistenceRuleInternal(
            final BeansServiceManager transaction,
            final DataPersistenceRule rule )
    {
        verifyNotFinalHealthyPermPersistenceIfInUse( rule, TargetHealth.INCLUDE_HEALTHY_TARGETS_ONLY );
        final boolean dataPolicyInUse = isDataPolicyInUse( rule.getDataPolicyId() );
        if ( dataPolicyInUse )
        {
            transaction.getService( BlobTapeService.class ).reclaimForDeletedPersistenceRule(
                    rule.getDataPolicyId(), rule.getStorageDomainId() );
            transaction.getService( BlobPoolService.class ).reclaimForDeletedPersistenceRule(
                    rule.getDataPolicyId(), rule.getStorageDomainId() );
        }

        transaction.getService( DataPersistenceRuleService.class ).delete( rule.getId() );
    }


    @Override
    synchronized public RpcFuture< ? > convertStorageDomainToDs3Target(
            final UUID storageDomainId,
            final UUID ds3TargetId )
    {
        final Set< DataPersistenceRule > persistenceRules =
                m_serviceManager.getRetriever( DataPersistenceRule.class ).retrieveAll(
                        DataPersistenceRule.STORAGE_DOMAIN_ID, storageDomainId ).toSet();
        for ( final StorageDomainMember member
                : m_serviceManager.getRetriever( StorageDomainMember.class ).retrieveAll(
                StorageDomainMember.STORAGE_DOMAIN_ID, storageDomainId ).toSet() )
        {
            if ( null != member.getTapePartitionId() )
            {
                final TapePartition tapePartition =
                        m_serviceManager.getRetriever( TapePartition.class ).attain(
                                member.getTapePartitionId() );
                if ( TapePartitionState.OFFLINE != tapePartition.getState() )
                {
                    throw new DataPlannerException(
                            GenericFailure.CONFLICT,
                            "Cannot convert a storage domain that contains online tape partitions." );
                }
            }
        }

        int count = 0;
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            final Set< BlobDs3Target > bts = new HashSet<>();
            try ( final EnhancedIterable< BlobPool > iterable =
                          m_serviceManager.getRetriever( BlobPool.class ).retrieveAll( Require.exists(
                                  BlobPool.POOL_ID,
                                  Require.exists( PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                                          Require.beanPropertyEquals(
                                                  StorageDomainMember.STORAGE_DOMAIN_ID,
                                                  storageDomainId ) ) ) ).toIterable() )
            {
                for ( final BlobPool bp : iterable )
                {
                    ++count;
                    bts.add( BeanFactory.newBean( BlobDs3Target.class )
                            .setBlobId( bp.getBlobId() ).setTargetId( ds3TargetId ) );
                    if ( 10000 == bts.size() )
                    {
                        commitBlobTargets( transaction, storageDomainId, bts );
                    }
                }
                commitBlobTargets( transaction, storageDomainId, bts );
            }

            try ( final EnhancedIterable< BlobTape > iterable =
                          m_serviceManager.getRetriever( BlobTape.class ).retrieveAll( Require.exists(
                                  BlobTape.TAPE_ID,
                                  Require.exists( PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                                          Require.beanPropertyEquals(
                                                  StorageDomainMember.STORAGE_DOMAIN_ID,
                                                  storageDomainId ) ) ) ).toIterable() )
            {
                for ( final BlobTape bt : iterable )
                {
                    ++count;
                    bts.add( BeanFactory.newBean( BlobDs3Target.class )
                            .setBlobId( bt.getBlobId() ).setTargetId( ds3TargetId ) );
                    if ( 10000 == bts.size() )
                    {
                        commitBlobTargets( transaction, storageDomainId, bts );
                    }
                }
                commitBlobTargets( transaction, storageDomainId, bts );
            }

            for ( final DataPersistenceRule rule : persistenceRules )
            {
                final Ds3DataReplicationRule replicationRule =
                        BeanFactory.newBean( Ds3DataReplicationRule.class );
                replicationRule.setDataPolicyId( rule.getDataPolicyId() );
                replicationRule.setTargetId( ds3TargetId );
                replicationRule.setState( rule.getState() );
                replicationRule.setType( ( DataPersistenceRuleType.PERMANENT == rule.getType() ) ?
                        DataReplicationRuleType.PERMANENT
                        : DataReplicationRuleType.RETIRED );
                transaction.getService( Ds3DataReplicationRuleService.class ).create( replicationRule );
                deleteDataPersistenceRuleInternal( transaction, rule );
            }

            LOG.warn( "Converted " + count + " blobs from storage domain " + storageDomainId
                    + " to DS3 target " + ds3TargetId + "." );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        return null;
    }


    private void commitBlobTargets(
            final BeansServiceManager transaction,
            final UUID storageDomainId,
            final Set< BlobDs3Target > bts )
    {
        if ( bts.isEmpty() )
        {
            return;
        }

        transaction.getService( BlobDs3TargetService.class ).migrate( storageDomainId, bts );
        bts.clear();
    }


    @Override
    synchronized public RpcFuture< UUID > createDs3DataReplicationRule( final Ds3DataReplicationRule rule )
    {
        return new RpcResponse<>( createDataReplicationRuleInternal(
                Ds3DataReplicationRule.class,
                rule ) );
    }


    private < R extends DataReplicationRule< R > > UUID createDataReplicationRuleInternal(
            final Class< R > replicationRuleType,
            final R rule )
    {
        try ( final NestableTransaction transaction = m_serviceManager.startNestableTransaction() )
        {
            if ( DataReplicationRuleType.RETIRED == rule.getType() )
            {
                throw new DataPlannerException(
                        GenericFailure.BAD_REQUEST,
                        "Only existing replication rules can be retired.  "
                                + "It is non-sensical to create a retired replication rule." );
            }

            if (isDataPolicyInUse( rule.getDataPolicyId() ) )
            {
                if ( transaction.getRetriever( DataPathBackend.class ).attain( Require.nothing() ).isIomEnabled() )
                {
                    if ( allowedToCreateRule( rule, transaction ) )
                    {
                        rule.setState( DataPlacementRuleState.INCLUSION_IN_PROGRESS );
                    }
                    else
                    {
                        throw new DataPlannerException(
                                GenericFailure.CONFLICT,
                                "There are outstanding PUT jobs for buckets that would be affected by this change. "
                                        + "Please allow these jobs to complete, or cancel them prior to adding a new "
                                        + "data replication rule." );
                    }
                }
                else
                {
                    throw new DataPlannerException(
                            GenericFailure.CONFLICT,
                            "Cannot perform the requested operation since the data policy "
                                    + "is being used by at least one bucket and IOM is not enabled." );
                }
            }
            validateDataReplicationRule( rule );
            transaction.getCreator( replicationRuleType ).create( rule );
            transaction.commitNestableTransaction();
            return rule.getId();
        }
    }

    private static boolean allowedToCreateRule( final DataPlacement<?> rule, final BeansServiceManager transaction )
    {
        final JobService jobService = transaction.getService( JobService.class );
        final Set< Bucket > affectedBuckets =
                new DataPolicyRM( rule.getDataPolicyId(), transaction ).getBuckets().toSet();
        final Set< UUID > affectedBucketIds = BeanUtils.toMap( affectedBuckets ).keySet();
        return jobService.retrieveAll(
                Require.all(
                        Require.beanPropertyEquals( JobObservable.REQUEST_TYPE, JobRequestType.PUT ),
                        Require.beanPropertyEqualsOneOf( JobObservable.BUCKET_ID, affectedBucketIds ) ) ).isEmpty();
    }


    @Override
    synchronized public RpcFuture< ? > modifyDs3DataReplicationRule(
            final Ds3DataReplicationRule rule,
            final String [] arrayPropertiesToModify )
    {
        modifyDataReplicationRuleInternal(
                Ds3DataReplicationRule.class,
                DegradedBlob.DS3_REPLICATION_RULE_ID,
                rule,
                arrayPropertiesToModify );
        return null;
    }


    private < R extends DataReplicationRule< R > > void modifyDataReplicationRuleInternal(
            final Class< R > replicationRuleType,
            final String degradedBlobReplicationRulePropertyName,
            final R rule,
            final String [] arrayPropertiesToModify )
    {
        validateDataReplicationRule( rule );

        final R original = m_serviceManager.getRetriever( replicationRuleType ).attain( rule.getId() );
        final Set< String > propertiesToModify = CollectionFactory.toSet( arrayPropertiesToModify );
        if ( propertiesToModify.contains( DataPersistenceRule.TYPE ) )
        {
            if ( DataReplicationRuleType.PERMANENT == rule.getType() )
            {
                verifyDataPolicyNotInUse( rule.getDataPolicyId() );
            }
            else //if retiring this rule
            {
                //NOTE: we should safely be able to include "inclusion in progress" rules in the health rule here.
                //That would allow us to retired a replication rule as long as some other rule is inclusion in progress
                //However, if we then added a "temporary" cloud persistence option in the future, we would risk
                //losing data if someone did set the retired rule to temporary. We have protections for this with
                //data persistence rules, but we can't write such protections for target since we don't have any such
                //feature right now. For now, we will leave the requirement more conservative to be safe.
                verifyNotFinalHealthyPermReplicationIfInUse( rule, TargetHealth.INCLUDE_HEALTHY_TARGETS_ONLY );
            }
        }

        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            transaction.getUpdater( replicationRuleType ).update(
                    rule, CollectionFactory.toArray( String.class, propertiesToModify ) );
            if ( DataReplicationRuleType.PERMANENT == original.getType()
                    && DataReplicationRuleType.PERMANENT != rule.getType() )
            {
                transaction.getService( DegradedBlobService.class ).deleteAllForReplicationRule(
                        degradedBlobReplicationRulePropertyName,
                        rule.getId() );
            }
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }


    private void validateDataReplicationRule( final DataReplicationRule< ? > rule )
    {
        if ( PublicCloudDataReplicationRule.class.isAssignableFrom( rule.getClass() ) )
        {
            validatePublicCloudDataReplicationRule( (PublicCloudDataReplicationRule< ? >)rule );
        }
    }


    private void validatePublicCloudDataReplicationRule( final PublicCloudDataReplicationRule< ? > rule )
    {
        if ( rule.getMaxBlobPartSizeInBytes() < PUBLIC_CLOUD_MIN_BLOB_PART_SIZE )
        {
            throw new DataPlannerException(
                    GenericFailure.BAD_REQUEST,
                    "The " + PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES
                            + " must be at least " + PUBLIC_CLOUD_MIN_BLOB_PART_SIZE + " bytes." );
        }
        if ( rule.getMaxBlobPartSizeInBytes() > PUBLIC_CLOUD_MAX_BLOB_PART_SIZE )
        {
            throw new DataPlannerException(
                    GenericFailure.BAD_REQUEST,
                    "The " + PublicCloudDataReplicationRule.MAX_BLOB_PART_SIZE_IN_BYTES
                            + " cannot exceed " + PUBLIC_CLOUD_MAX_BLOB_PART_SIZE + " bytes." );
        }
    }


    @Override
    synchronized public RpcFuture< ? > deleteDs3DataReplicationRule( final UUID ruleId )
    {
        deleteDataReplicationRuleInternal(
                Ds3DataReplicationRule.class,
                BlobDs3TargetService.class,
                ruleId );
        return null;
    }


    private < R extends DataReplicationRule< R >, BTS extends BlobTargetService< ?, R > >
    void deleteDataReplicationRuleInternal(
            final Class< R > replicationRuleType,
            final Class< BTS > blobTargetServiceType,
            final UUID ruleId )
    {
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            final R rule = transaction.getRetriever( replicationRuleType ).attain( ruleId );
            verifyNotFinalHealthyPermReplicationIfInUse( rule, TargetHealth.INCLUDE_HEALTHY_TARGETS_ONLY );
            final boolean dataPolicyInUse = isDataPolicyInUse( rule.getDataPolicyId() );
            if ( dataPolicyInUse )
            {
                transaction.getService( blobTargetServiceType ).reclaimForDeletedReplicationRule(
                        rule.getDataPolicyId(),
                        rule );
            }

            transaction.getDeleter( replicationRuleType ).delete( rule.getId() );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }


    @Override
    synchronized public RpcFuture< UUID > createAzureDataReplicationRule(
            final AzureDataReplicationRule rule )
    {
        return new RpcResponse<>( createDataReplicationRuleInternal(
                AzureDataReplicationRule.class,
                rule ) );
    }


    @Override
    synchronized public RpcFuture< ? > modifyAzureDataReplicationRule(
            final AzureDataReplicationRule rule,
            final String [] arrayPropertiesToModify )
    {
        modifyDataReplicationRuleInternal(
                AzureDataReplicationRule.class,
                DegradedBlob.AZURE_REPLICATION_RULE_ID,
                rule,
                arrayPropertiesToModify );
        return null;
    }


    @Override
    synchronized public RpcFuture< ? > deleteAzureDataReplicationRule( final UUID ruleId )
    {
        deleteDataReplicationRuleInternal(
                AzureDataReplicationRule.class,
                BlobAzureTargetService.class,
                ruleId );
        return null;
    }


    @Override
    synchronized public RpcFuture< UUID > createS3DataReplicationRule( final S3DataReplicationRule rule )
    {
        final DataPolicy dataPolicy = m_serviceManager.getRetriever( DataPolicy.class )
                .attain( rule.getDataPolicyId() );
        final S3Target target = m_serviceManager.getRetriever( S3Target.class )
                .attain( rule.getTargetId() );
        if ( null == target.getDataPathEndPoint() &&
                0 < m_serviceManager.getRetriever( S3DataReplicationRule.class ).getCount(
                        Require.all(
                                Require.beanPropertyEquals( DataPlacement.DATA_POLICY_ID, rule.getDataPolicyId() ),
                                Require.exists( DataReplicationRule.TARGET_ID,
                                        Require.all(
                                                Require.beanPropertyEquals(
                                                        PublicCloudReplicationTarget.CLOUD_BUCKET_PREFIX,
                                                        target.getCloudBucketPrefix() ),
                                                Require.beanPropertyEquals(
                                                        PublicCloudReplicationTarget.CLOUD_BUCKET_SUFFIX,
                                                        target.getCloudBucketSuffix() ) ) ) ) ) )
        {
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "DataPolicy \"" + dataPolicy.getName() + "\" already replicates to an "
                            + "AWS target. If you really meant to replicate to AWS twice,"
                            + "please give target \"" + target.getName() + "\" unique cloud"
                            + " bucket prefix/suffix settings to prevent name collisions.");
        }
        return new RpcResponse<>( createDataReplicationRuleInternal(
                S3DataReplicationRule.class,
                rule ) );
    }


    @Override
    synchronized public RpcFuture< ? > modifyS3DataReplicationRule(
            final S3DataReplicationRule rule,
            final String [] arrayPropertiesToModify )
    {
        modifyDataReplicationRuleInternal(
                S3DataReplicationRule.class,
                DegradedBlob.S3_REPLICATION_RULE_ID,
                rule,
                arrayPropertiesToModify );
        return null;
    }


    @Override
    synchronized public RpcFuture< ? > deleteS3DataReplicationRule( final UUID ruleId )
    {
        deleteDataReplicationRuleInternal(
                S3DataReplicationRule.class,
                BlobS3TargetService.class,
                ruleId );
        return null;
    }


    @Override
    synchronized public RpcFuture< UUID > modifyStorageDomain(
            final StorageDomain storageDomain,
            final String [] propertiesToUpdate )
    {
        if ( CollectionFactory.toSet( propertiesToUpdate ).contains(
                StorageDomain.LTFS_FILE_NAMING ) )
        {
            if ( 0 < m_serviceManager.getRetriever( Bucket.class ).getCount( Require.exists(
                    Bucket.DATA_POLICY_ID,
                    Require.exists(
                            DataPersistenceRule.class,
                            DataPlacement.DATA_POLICY_ID,
                            Require.beanPropertyEquals(
                                    DataPersistenceRule.STORAGE_DOMAIN_ID,
                                    storageDomain.getId() ) ) ) ) )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Cannot modify the " + StorageDomain.LTFS_FILE_NAMING
                                + " since buckets are using the storage domain." );
            }
            if ( 0 < m_serviceManager.getRetriever( DataPolicy.class ).getCount( Require.all(
                    Require.not( Require.beanPropertyEquals( DataPolicy.VERSIONING, VersioningLevel.NONE ) ),
                    Require.exists(
                            DataPersistenceRule.class,
                            DataPlacement.DATA_POLICY_ID,
                            Require.beanPropertyEquals(
                                    DataPersistenceRule.STORAGE_DOMAIN_ID,
                                    storageDomain.getId() ) ) ) ) )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Cannot modify the " + StorageDomain.LTFS_FILE_NAMING
                                + " since data policies with versioning enabled are using the storage domain." );
            }
            if ( 0 < m_serviceManager.getRetriever( Tape.class ).getCount(
                    Require.exists( PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID,
                            Require.beanPropertyEquals( StorageDomainMember.STORAGE_DOMAIN_ID,
                                    storageDomain.getId() ) ) ) )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Cannot modify the " + StorageDomain.LTFS_FILE_NAMING
                                + " since tapes are allocated to the storage domain." );
            }
        }

        m_serviceManager.getService( StorageDomainService.class ).update(
                storageDomain, propertiesToUpdate );
        return new RpcResponse<>();
    }


    @Override
    synchronized public RpcFuture< UUID > createStorageDomainMember( final StorageDomainMember member )
    {
        // new member is tape partition?
        if ( null != member.getTapePartitionId() )
        {
            // Don't allow adding tape partition if we have disk pools in the storage domain
            int poolCount = m_serviceManager.getRetriever( StorageDomainMember.class ).getCount( Require.all(
                    Require.not(Require.beanPropertyEquals(StorageDomainMember.POOL_PARTITION_ID, null)),
                    Require.beanPropertyEquals( DataPersistenceRule.STORAGE_DOMAIN_ID,
                            member.getStorageDomainId() ) ) );
            if ( poolCount > 0 )
            {
                throw new DataPlannerException(
                        GenericFailure.BAD_REQUEST,
                        "Tape partition cannot be added to a storage domain that contains disk pools." );
            }

            if ( null == member.getTapeType()
                    || !member.getTapeType().canContainData()
                    || TapeType.UNKNOWN == member.getTapeType() )
            {
                throw new DataPlannerException(
                        GenericFailure.BAD_REQUEST,
                        "Invalid tape type: " + member.getTapeType() );
            }
            final TapeDriveType driveType =
                    new StorageDomainMemberRM( member, m_serviceManager ).getTapePartition().getDriveType();
            if ( !driveType.isReadSupported( member.getTapeType() ) )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Invalid tape type: " + member.getTapeType() + " for partition with drive(s) of type "
                                + driveType );
            }
            if ( 0 < m_serviceManager.getRetriever( DataPersistenceRule.class ).getCount( Require.all(
                    Require.beanPropertyEquals( DataPersistenceRule.TYPE, DataPersistenceRuleType.TEMPORARY ),
                    Require.beanPropertyEquals(
                            DataPersistenceRule.STORAGE_DOMAIN_ID,
                            member.getStorageDomainId() ) ) ) )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Cannot add tape to a storage domain that is being used "
                                + "as a temporary persistence target." );
            }
        }

        // new member is disk pool partition?
        // Don't allow adding a pool partition if we have tape partitions already in the storage domain
        if ( null != member.getPoolPartitionId() )
        {
            int tapeCount = m_serviceManager.getRetriever( StorageDomainMember.class ).getCount( Require.all(
                    Require.not( Require.beanPropertyEquals( StorageDomainMember.TAPE_PARTITION_ID, null ) ),
                    Require.beanPropertyEquals( DataPersistenceRule.STORAGE_DOMAIN_ID,
                            member.getStorageDomainId() ) ) );
            if ( tapeCount > 0 )
            {
                throw new DataPlannerException(
                        GenericFailure.BAD_REQUEST,
                        "Disk pool cannot be added to a storage domain that contains tape partitions." );
            }
            if (PoolType.NEARLINE == m_serviceManager.getRetriever( PoolPartition.class ).attain(
                    member.getPoolPartitionId() ).getType() )
            {
                if ( 0 < m_serviceManager.getRetriever( DataPersistenceRule.class ).getCount( Require.all(
                        Require.beanPropertyEquals( DataPersistenceRule.TYPE, DataPersistenceRuleType.TEMPORARY ),
                        Require.beanPropertyLessThan(
                                DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN,
                                Integer.valueOf( MIN_MIN_DAYS_TO_RETAIN_FOR_NEARLINE_POOL ) ),
                        Require.beanPropertyEquals(
                                DataPersistenceRule.STORAGE_DOMAIN_ID,
                                member.getStorageDomainId() ) ) ) )
                {
                    throw new DataPlannerException(
                            GenericFailure.CONFLICT,
                            "Cannot add " + PoolType.NEARLINE + " pool to a storage domain that is being used "
                                    + "as a temporary persistence target with a "
                                    + DataPersistenceRule.MINIMUM_DAYS_TO_RETAIN + " less than "
                                    + MIN_MIN_DAYS_TO_RETAIN_FOR_NEARLINE_POOL + " days." );
                }
            }
        }

        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            transaction.getService( StorageDomainMemberService.class ).create( member );
            transaction.getService( StorageDomainMemberService.class ).ensureWritePreferencesValid();
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }

        return new RpcResponse<>( member.getId() );
    }


    @Override
    synchronized public RpcFuture< ? > modifyStorageDomainMember(
            final StorageDomainMember storageDomainMember,
            final String [] propertiesToUpdate )
    {


        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            final StorageDomainMember existingMember =
                    transaction.getRetriever( StorageDomainMember.class ).attain( storageDomainMember.getId() );
            if ( CollectionFactory.toSet( propertiesToUpdate ).contains(
                    StorageDomainMember.WRITE_PREFERENCE ) )
            {
                if ( WritePreferenceLevel.NEVER_SELECT == storageDomainMember.getWritePreference() )
                {
                    verifyNotFinalActiveStorageDomainMemberIfInUse( existingMember, transaction );
                }
                transaction.getService( StorageDomainMemberService.class ).update(
                        storageDomainMember,
                        StorageDomainMember.WRITE_PREFERENCE );
                transaction.getService( StorageDomainMemberService.class ).ensureWritePreferencesValid();
            }

            if ( CollectionFactory.toSet( propertiesToUpdate ).contains(
                    StorageDomainMember.AUTO_COMPACTION_THRESHOLD ) )
            {
                if ( null != storageDomainMember.getAutoCompactionThreshold()
                        && existingMember.getTapeType().getMinimumAutoCompactionThreshold()
                        > storageDomainMember.getAutoCompactionThreshold() )
                {
                    throw new DataPlannerException(
                            GenericFailure.CONFLICT,
                            "Auto-Compaction threshold must be greater than or equal to the tape type's minimum: " +
                                    existingMember.getTapeType().getMinimumAutoCompactionThreshold() + "%" );
                }
                transaction.getService( StorageDomainMemberService.class ).update(
                        storageDomainMember,
                        StorageDomainMember.AUTO_COMPACTION_THRESHOLD );
            }


            if ( CollectionFactory.toSet( propertiesToUpdate ).contains(
                    StorageDomainMember.STATE ) )
            {
                if ( storageDomainMember.getState() == existingMember.getState() )
                {
                    LOG.warn( "Storage domain member " + storageDomainMember.getId() + " is already in state " +
                            storageDomainMember.getState().name() + ".");
                }
                else
                {
                    if ( StorageDomainMemberState.EXCLUSION_IN_PROGRESS == storageDomainMember.getState() )
                    {
                        verifyNotFinalActiveStorageDomainMemberIfInUse( existingMember, transaction );
                        if ( !transaction.getRetriever( DataPathBackend.class )
                                .attain( Require.nothing() ).isIomEnabled() )
                        {
                            throw new FailureTypeObservableException(
                                    GenericFailure.CONFLICT,
                                    "Cannot exclude storage domain member because IOM is not enabled." );
                        }
                    }
                    transaction.getService( StorageDomainMemberService.class ).update(
                            storageDomainMember,
                            StorageDomainMember.STATE );
                }
            }

            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        return null;
    }


    @Override
    synchronized public RpcFuture< ? > deleteStorageDomainMember( final UUID storageDomainMemberId )
    {
        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            final StorageDomainMember member =
                    transaction.getRetriever( StorageDomainMember.class ).attain( storageDomainMemberId );
            verifyNotFinalActiveStorageDomainMemberIfInUse( member, transaction );
            final StorageDomainMemberRM sdm = new StorageDomainMemberRM( member, transaction );


            for ( final Tape t : sdm.getTapes().toSet() )
            {
                transaction.getService( TapeService.class ).updateAssignment( t.getId() );
            }
            for ( final Pool p : sdm.getPools().toSet() )
            {
                transaction.getService( PoolService.class ).updateAssignment( p.getId() );
            }

            if ( sdm.getTapes().isEmpty() && sdm.getPools().isEmpty() )
            {
                transaction.getService( StorageDomainMemberService.class ).delete( storageDomainMemberId );
            }
            else
            {
                if ( StorageDomainMemberState.EXCLUSION_IN_PROGRESS == member.getState() )
                {
                    throw new FailureTypeObservableException(
                            GenericFailure.CONFLICT,
                            "Cannot delete since storage domain member has at least one tape or disk pool that still"
                                    + " has data allocated to it." );
                }
                throw new FailureTypeObservableException(
                        GenericFailure.CONFLICT,
                        "Cannot delete since storage domain member has at least one tape or disk pool that still"
                                + " has data allocated to it. If you would like to migrate data off that media, please"
                                + " ensure that IOM is enabled exclude this storage domain member. It will be automatically"
                                + " deleted following migration and database backup." );
            }
            transaction.commitTransaction();
            return null;
        }
        finally
        {
            transaction.closeTransaction();
        }
    }


    private static void verifyNotFinalActiveStorageDomainMemberIfInUse(
            final StorageDomainMember member,
            final BeansServiceManager bsm )
    {
        final Set< StorageDomainMember > activeMembers =
                getStorageDomainMembers( member.getStorageDomainId(), true, bsm );
        if ( 1 == activeMembers.size() && member.getId().equals( activeMembers.iterator().next().getId() ) )
        {
            verifyStorageDomainNotInUse( member.getStorageDomainId(), bsm );
        }
    }


    private static boolean isStorageDomainInUse( final UUID storageDomainId, final BeansServiceManager bsm )
    {
        return ( 0 < bsm.getRetriever( DataPersistenceRule.class ).getCount(
                DataPersistenceRule.STORAGE_DOMAIN_ID, storageDomainId ) );
    }


    private static void verifyStorageDomainNotInUse( final UUID storageDomainId, final BeansServiceManager bsm )
    {
        if ( isStorageDomainInUse( storageDomainId, bsm ) )
        {
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "Cannot perform the requested operation since the storage domain "
                            + "is being used by at least one data policy." );
        }
    }


    @Override
    public RpcFuture< ? > modifyPool( final UUID poolId, final UUID newPoolPartitionId )
    {
        final Pool pool = m_serviceManager.getRetriever( Pool.class ).attain( poolId );
        final UUID storageDomainId;
        if (null == pool.getStorageDomainMemberId() )
        {
            storageDomainId = null;
        }
        else
        {
            storageDomainId =
                    new PoolRM( pool, m_serviceManager ).getStorageDomainMember().unwrap().getStorageDomainId();
        }

        if ( null != pool.getStorageDomainMemberId()
                && null == m_serviceManager.getRetriever( StorageDomainMember.class ).retrieve(
                Require.all(
                        Require.beanPropertyEquals(
                                StorageDomainMember.STORAGE_DOMAIN_ID, storageDomainId ),
                        Require.beanPropertyEquals(
                                StorageDomainMember.POOL_PARTITION_ID, newPoolPartitionId ) ) ) )
        {
            throw new DataPlannerException(
                    GenericFailure.CONFLICT,
                    "Pool is assigned to storage domain " + storageDomainId
                            + " and cannot be assigned to another partition not a member of the storage domain." );
        }

        if ( null != newPoolPartitionId )
        {
            final PoolPartition partition =
                    m_serviceManager.getRetriever( PoolPartition.class ).attain( newPoolPartitionId );
            if ( pool.getType() != partition.getType() )
            {
                throw new DataPlannerException(
                        GenericFailure.CONFLICT,
                        "Pool cannot be assigned to partition since the pool is of type " + pool.getType()
                                + ", but the partition is of type " + partition.getType() + "." );
            }
        }

        m_serviceManager.getService( PoolService.class ).update(
                pool.setPartitionId( newPoolPartitionId ),
                Pool.PARTITION_ID );
        return null;
    }


    private final BeansServiceManager m_serviceManager;

    private final static long PUBLIC_CLOUD_MIN_BLOB_PART_SIZE = 100 * 1024L * 1024;
    private final static long PUBLIC_CLOUD_MAX_BLOB_PART_SIZE = 1024 * 1024 * 1024L * 1024;
    private final static Set< String > DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME;
    private final static int MIN_MIN_DAYS_TO_RETAIN_FOR_NEARLINE_POOL = 1;
    private final static Logger LOG = Logger.getLogger( DataPolicyManagementResourceImpl.class );
    static
    {
        DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME = new HashSet<>();
        DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.add( NameObservable.NAME );
        DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.add( DataPolicy.BLOBBING_ENABLED );
        DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.add( DataPolicy.DEFAULT_BLOB_SIZE );
        DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.add( DataPolicy.DEFAULT_GET_JOB_PRIORITY );
        DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.add( DataPolicy.DEFAULT_PUT_JOB_PRIORITY );
        DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.add( DataPolicy.DEFAULT_VERIFY_JOB_PRIORITY );
        DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.add( DataPolicy.DEFAULT_VERIFY_AFTER_WRITE );
        DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.add( DataPolicy.END_TO_END_CRC_REQUIRED );
        DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.add( DataPolicy.REBUILD_PRIORITY );
        DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.add( DataPolicy.CREATION_DATE );
        DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.add( DataPolicy.ALWAYS_FORCE_PUT_JOB_CREATION );
        DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.add( DataPolicy.ALWAYS_MINIMIZE_SPANNING_ACROSS_MEDIA );
        DATA_POLICY_PROPERTIES_MODIFIABLE_AT_ANY_TIME.add( DataPolicy.MAX_VERSIONS_TO_KEEP );
    }
}
