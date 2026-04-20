/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.pool;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.orm.PoolRM;
import com.spectralogic.s3.common.platform.persistencetarget.PersistenceTargetUtil;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.db.service.api.NestableTransaction;

final class PoolServiceImpl extends BaseService< Pool > implements PoolService
{
    PoolServiceImpl()
    {
        super( Pool.class );
    }
    
    
    public long [] getAvailableSpacesForBucket( final UUID bucketId, final UUID storageDomainId )
    {
        final List< Pool > pools = retrieveAll(
                PersistenceTargetUtil.filterForWritablePools(
                        PersistenceTargetUtil.getIsolatedBucketId( 
                                bucketId, 
                                storageDomainId,
                                getServiceManager() ), 
                        storageDomainId ) ).toList();
        Collections.sort( pools, new BeanComparator<>( Pool.class, PoolObservable.AVAILABLE_CAPACITY ) );

        final long [] retval = new long[ pools.size() ];
        for ( int i = 0; i < retval.length; ++i )
        {
            retval[ i ] = pools.get( i ).getAvailableCapacity();
        }
        return retval;
    }
    
    
    public void updateDates( final UUID poolId, final PoolAccessType accessType )
    {
        final Date date = new Date();
        final Pool pool = BeanFactory.newBean( Pool.class )
                .setLastAccessed( date ).setLastModified( date ).setLastVerified( date );
        pool.setId( poolId );
        
        switch ( accessType )
        {
            case ACCESSED:
                update( pool, PersistenceTarget.LAST_ACCESSED );
                break;
            case MODIFIED:
                pool.setLastVerified( null );
                update( pool, 
                        PersistenceTarget.LAST_ACCESSED,
                        PersistenceTarget.LAST_MODIFIED, 
                        PersistenceTarget.LAST_VERIFIED );
                break;
            case VERIFIED:
                update( pool, PersistenceTarget.LAST_ACCESSED, PersistenceTarget.LAST_VERIFIED );
                break;
            default:
                throw new UnsupportedOperationException( "No code for: " + accessType );
        }
    }
    
    
    public void updateAssignment( final UUID poolId )
    {
    	try( final NestableTransaction transaction = getServiceManager().startNestableTransaction() )
    	{
	    	final PoolRM pool = new PoolRM( poolId, transaction ); 
	    	if ( pool.getBlobPools().isEmpty()
	    			&& null != pool.unwrap().getStorageDomainMemberId()
	    			&& !pool.getStorageDomainMember().getStorageDomain().isSecureMediaAllocation() )
	    	{
	    		LOG.warn( "Pool " + pool.getId() + " (" + pool.getName() + ") is no longer referenced by any blob pool"
	    				+ " records and can be unassigned from storage domain member "
						+ pool.getStorageDomainMember() + ".");
	    		update( pool.unwrap().setStorageDomainMemberId( null ), PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
	    	}
    	}
    }
}
