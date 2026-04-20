/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.HashSet;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.CapacitySummaryContainer;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainCapacitySummary;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.ds3.SystemCapacitySummary;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.platform.persistencetarget.PersistenceTargetUtil;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;

final class StorageDomainServiceImpl 
    extends BaseService< StorageDomain > implements StorageDomainService
{
    StorageDomainServiceImpl()
    {
        super( StorageDomain.class );
    }
    
    
    @Override
    public CapacitySummaryContainer getCapacitySummary( 
            final UUID bucketId, 
            final UUID storageDomainId, 
            final WhereClause tapeFilter,
            final WhereClause poolFilter )
    {
        Validations.verifyNotNull( "Bucket", bucketId );
        Validations.verifyNotNull( "Storage domain", storageDomainId );
        
        final StorageDomainCapacitySummary tapeSummary = 
                BeanFactory.newBean( StorageDomainCapacitySummary.class );
        populateTapeCapacitySummary( tapeSummary, bucketId, storageDomainId, tapeFilter );
        
        final StorageDomainCapacitySummary poolSummary = 
                BeanFactory.newBean( StorageDomainCapacitySummary.class );
        populatePoolCapacitySummary( poolSummary, bucketId, storageDomainId, poolFilter );
        
        return wrapCapacitySummary( tapeSummary, poolSummary );
    }
    
    
    @Override
    public CapacitySummaryContainer getCapacitySummary( 
            final UUID storageDomainId, 
            final WhereClause tapeFilter,
            final WhereClause poolFilter )
    {
        Validations.verifyNotNull( "Storage domain", storageDomainId );
        
        final StorageDomainCapacitySummary tapeSummary = 
                BeanFactory.newBean( StorageDomainCapacitySummary.class );
        populateTapeCapacitySummary( tapeSummary, null, storageDomainId, tapeFilter );
        
        final StorageDomainCapacitySummary poolSummary = 
                BeanFactory.newBean( StorageDomainCapacitySummary.class );
        populatePoolCapacitySummary( poolSummary, null, storageDomainId, poolFilter );
        
        return wrapCapacitySummary( tapeSummary, poolSummary );
    }
    
    
    @Override
    public CapacitySummaryContainer getCapacitySummary(
            final WhereClause tapeFilter,
            final WhereClause poolFilter )
    {
        final SystemCapacitySummary tapeSummary = BeanFactory.newBean( SystemCapacitySummary.class );
        populateTapeCapacitySummary( tapeSummary, null, null, tapeFilter );
        tapeSummary.setPhysicalAvailable( getDataManager().getSum(
                Tape.class, 
                Tape.TOTAL_RAW_CAPACITY,
                Require.all( 
                        tapeFilter,
                        PersistenceTargetUtil.filterForWritableTapes(
                                null, null, 1, new HashSet< UUID >(), null, true ) ) ) );
        
        final SystemCapacitySummary poolSummary = BeanFactory.newBean( SystemCapacitySummary.class );
        populatePoolCapacitySummary( poolSummary, null, null, poolFilter );
        poolSummary.setPhysicalAvailable( getDataManager().getSum(
                Pool.class, 
                PoolObservable.TOTAL_CAPACITY,
                Require.all( 
                        poolFilter,
                        PersistenceTargetUtil.filterForWritablePools( 
                                null, null, 0, new HashSet< UUID >(), true ) ) ) );

        return wrapCapacitySummary( tapeSummary, poolSummary );
    }
    
    
    public UUID selectAppropriateStorageDomainMember(
            final PersistenceTarget< ? > pt,
            final UUID storageDomainId )
    {
        if ( pt instanceof Tape )
        {
            final Tape tape = (Tape)pt;
            final StorageDomainMember sdm = getServiceManager().getRetriever( StorageDomainMember.class ).retrieve(
                    Require.all(
                            Require.beanPropertyEquals( StorageDomainMember.STORAGE_DOMAIN_ID, storageDomainId ),
                            Require.beanPropertyEquals( StorageDomainMember.TAPE_PARTITION_ID, tape.getPartitionId() ),
                            Require.beanPropertyEquals( StorageDomainMember.TAPE_TYPE, tape.getType() )
                            ) );
            if ( null == sdm )
            {
                throw new DaoException( 
                        GenericFailure.CONFLICT, 
                        "No storage domain member exists that can legally link tape \"" + tape.getBarCode() +
                        "\" to storage domain " + storageDomainId ); 
            }
            return sdm.getId();
        }
        else if ( pt instanceof Pool )
        {
            final Pool pool = (Pool)pt;
            final StorageDomainMember sdm = getServiceManager().getRetriever( StorageDomainMember.class ).retrieve(
                    Require.all(
                            Require.beanPropertyEquals( StorageDomainMember.STORAGE_DOMAIN_ID, storageDomainId ),
                            Require.beanPropertyEquals( StorageDomainMember.POOL_PARTITION_ID, pool.getPartitionId() )
                            ) );
            if ( null == sdm )
            {
                throw new DaoException( 
                        GenericFailure.CONFLICT, 
                        "No storage domain member exists that can legally link pool \"" + pool.getName() +
                        "\" to storage domain " + storageDomainId ); 
            }
            return sdm.getId();
        }
        else
        {
            throw new IllegalStateException(pt.getClass() + " is not a valid persistence target ");
        }
    }
    
    
    @Override
    final public StorageDomain retrieve( final String identifier )
    {
        if ( isUUID( identifier ) )
        {
            return retrieve( UUID.fromString( identifier ) );
        }
        return retrieve( NameObservable.NAME, identifier );
    }
    
    
    @Override
    final public StorageDomain attain( final String identifier )
    {
        if ( isUUID( identifier ) )
        {
            return attain( UUID.fromString( identifier ) );
        }
        return attain( NameObservable.NAME, identifier );
    }
    
    
    private boolean isUUID ( final String id )
    {
        return id.matches( "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$" );
    }
    
    
    private CapacitySummaryContainer wrapCapacitySummary( 
            final StorageDomainCapacitySummary tapeSummary,
            final StorageDomainCapacitySummary poolSummary )
    {
        final CapacitySummaryContainer retval = BeanFactory.newBean( CapacitySummaryContainer.class )
                .setTape( tapeSummary )
                .setPool( poolSummary );
        return retval;
    }
    
    
    private StorageDomainCapacitySummary populateTapeCapacitySummary(
            final StorageDomainCapacitySummary summary,
            final UUID bucketId,
            final UUID storageDomainId,
            final WhereClause tapeFilter )
    {
        final WhereClause physicalAllocatedFilter = 
                getPhysicalAllocatedFilter( bucketId, storageDomainId, tapeFilter );
        summary.setPhysicalAllocated( getDataManager().getSum( 
                Tape.class,
                Tape.TOTAL_RAW_CAPACITY,
                physicalAllocatedFilter ) );
        summary.setPhysicalFree( getDataManager().getSum( 
                Tape.class,
                Tape.AVAILABLE_RAW_CAPACITY,
                Require.all( 
                        physicalAllocatedFilter,
                        PersistenceTargetUtil.filterForWritableTapes() ) ) );
        summary.setPhysicalUsed( summary.getPhysicalAllocated() - getDataManager().getSum( 
                Tape.class,
                Tape.AVAILABLE_RAW_CAPACITY,
                physicalAllocatedFilter ) );
        
        return summary;
    }
    
    
    private StorageDomainCapacitySummary populatePoolCapacitySummary(
            final StorageDomainCapacitySummary summary,
            final UUID bucketId,
            final UUID storageDomainId, 
            final WhereClause poolFilter )
    {
        final WhereClause physicalAllocatedFilter = 
                getPhysicalAllocatedFilter( bucketId, storageDomainId, poolFilter );
        summary.setPhysicalAllocated( getDataManager().getSum( 
                Pool.class,
                PoolObservable.TOTAL_CAPACITY,
                physicalAllocatedFilter ) );
        summary.setPhysicalFree( getDataManager().getSum( 
                Pool.class,
                PoolObservable.AVAILABLE_CAPACITY,
                Require.all( 
                        physicalAllocatedFilter,
                        PersistenceTargetUtil.filterForWritablePools() ) ) );
        summary.setPhysicalUsed(
                summary.getPhysicalAllocated() 
                - getDataManager().getSum( 
                        Pool.class,
                        PoolObservable.AVAILABLE_CAPACITY,
                        physicalAllocatedFilter )
                - getDataManager().getSum( 
                        Pool.class,
                        PoolObservable.RESERVED_CAPACITY,
                        physicalAllocatedFilter ) );
        
        return summary;
    }
    
    
    private WhereClause getPhysicalAllocatedFilter(
            final UUID bucketId,
            final UUID storageDomainId,
            final WhereClause persistenceTargetFilter )
    {
        final UUID isolatedBucketId = ( null == bucketId ) ?
                null
                : PersistenceTargetUtil.getIsolatedBucketId( 
                        bucketId, storageDomainId, getServiceManager() );
        return Require.all( 
                persistenceTargetFilter,
                ( null == bucketId ) ?
                        null
                        : Require.beanPropertyEquals( PersistenceTarget.BUCKET_ID, isolatedBucketId ),
                ( null == storageDomainId ) ? 
                        Require.not( Require.beanPropertyEquals( 
                                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, null ) )
                        : Require.exists(
                                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID, 
                                Require.beanPropertyEquals(
                                        StorageDomainMember.STORAGE_DOMAIN_ID,
                                        storageDomainId) ) );
    }
}
