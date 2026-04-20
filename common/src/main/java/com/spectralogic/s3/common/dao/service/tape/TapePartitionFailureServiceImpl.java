/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailuresImpl;
import com.spectralogic.s3.common.dao.service.shared.BaseFailureService;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.ExceptionUtil;

final class TapePartitionFailureServiceImpl 
    extends BaseFailureService< TapePartitionFailure > implements TapePartitionFailureService
{
    TapePartitionFailureServiceImpl()
    {
        super( TapePartitionFailure.class );
    }
    
    
    public void create( 
            final UUID partitionId, 
            final TapePartitionFailureType type,
            final Throwable t,
            final Integer minMinutesSinceLastFailureOfSameType )
    {
        create( partitionId, 
                type, 
                ExceptionUtil.getReadableMessage( t ), 
                minMinutesSinceLastFailureOfSameType );
    }
    
    
    public void create( 
            final UUID partitionId, 
            final TapePartitionFailureType type,
            final String error,
            final Integer minMinutesSinceLastFailureOfSameType )
    {
        create( BeanFactory.newBean( TapePartitionFailure.class )
                .setErrorMessage( error )
                .setPartitionId( partitionId )
                .setType( type ), minMinutesSinceLastFailureOfSameType );
    }
    
    
    public void deleteAll( final UUID partitionId, final TapePartitionFailureType type )
    {
        deleteAll( Require.all( 
                Require.beanPropertyEquals( TapePartitionFailure.PARTITION_ID, partitionId ),
                Require.beanPropertyEquals( Failure.TYPE, type ) ) );
    }
    
    
    public void deleteAll( final UUID partitionId )
    {
        deleteAll( Require.beanPropertyEquals( TapePartitionFailure.PARTITION_ID, partitionId ) );
    }
    
    
    public ActiveFailures startActiveFailures( final UUID partitionId, final TapePartitionFailureType type )
    {
        return new ActiveFailuresImpl<>( 
                this,
                BeanFactory.newBean( TapePartitionFailure.class )
                    .setPartitionId( partitionId ).setType( type ) );
    }
}
