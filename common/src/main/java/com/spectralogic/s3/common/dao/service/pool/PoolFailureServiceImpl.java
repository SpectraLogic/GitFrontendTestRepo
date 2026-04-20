/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.pool;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.pool.PoolFailure;
import com.spectralogic.s3.common.dao.domain.pool.PoolFailureType;
import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailuresImpl;
import com.spectralogic.s3.common.dao.service.shared.BaseFailureService;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.ExceptionUtil;

final class PoolFailureServiceImpl extends BaseFailureService< PoolFailure > implements PoolFailureService
{
    PoolFailureServiceImpl()
    {
        super( PoolFailure.class );
    }
    
    
    public void create( final UUID poolId, final PoolFailureType type, final Throwable t )
    {
        create( poolId, type, ExceptionUtil.getReadableMessage( t ) );
    }
    
    
    public void create( final UUID poolId, final PoolFailureType type, final String error )
    {
        create( BeanFactory.newBean( PoolFailure.class )
                .setErrorMessage( error )
                .setPoolId( poolId )
                .setType( type ) );
    }
    
    
    public void deleteAll( final UUID poolId )
    {
        deleteAll( Require.beanPropertyEquals( PoolFailure.POOL_ID, poolId ) );
    }
    
    
    public void deleteAll( final UUID poolId, final PoolFailureType failureType )
    {
        deleteAll( Require.all( 
                Require.beanPropertyEquals( PoolFailure.POOL_ID, poolId ),
                Require.beanPropertyEquals( Failure.TYPE, failureType ) ) );
    }
    
    
    public ActiveFailures startActiveFailures( final UUID poolId, final PoolFailureType type )
    {
        return new ActiveFailuresImpl<>( 
                this, BeanFactory.newBean( PoolFailure.class ).setPoolId( poolId ).setType( type ) );
    }
}
