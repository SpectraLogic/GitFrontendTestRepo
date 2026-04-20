/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.target;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.common.dao.domain.target.TargetFailure;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.dao.service.shared.BaseFailureService;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.ExceptionUtil;

abstract class BaseTargetFailureService
    < T extends DatabasePersistable & TargetFailure< T > > extends BaseFailureService< T >
{
    protected BaseTargetFailureService( final Class< T > clazz )
    {
        super( clazz );
    }
    

    public void create( 
            final UUID targetId, 
            final TargetFailureType type,
            final Throwable t,
            final Integer minMinutesSinceLastFailureOfSameType )
    {
        create( 
                targetId, 
                type, 
                ExceptionUtil.getReadableMessage( t ),
                minMinutesSinceLastFailureOfSameType );
    }


    public void create( 
            final UUID targetId, 
            final TargetFailureType type,
            final String error,
            final Integer minMinutesSinceLastFailureOfSameType )
    {
        final T failure = BeanFactory.newBean( getServicedType() );
        failure.setErrorMessage( error );
        failure.setTargetId( targetId );
        ( (TargetFailure< ? >)failure ).setType( type );
        create( failure, minMinutesSinceLastFailureOfSameType );
    }


    public void deleteAll( final UUID targetId, final TargetFailureType type )
    {
        deleteAll( Require.all( 
                Require.beanPropertyEquals( TargetFailure.TARGET_ID, targetId ),
                Require.beanPropertyEquals( Failure.TYPE, type ) ) );
    }


    public void deleteAll( final UUID targetId )
    {
        deleteAll( Require.beanPropertyEquals( TargetFailure.TARGET_ID, targetId ) );
    }
}
