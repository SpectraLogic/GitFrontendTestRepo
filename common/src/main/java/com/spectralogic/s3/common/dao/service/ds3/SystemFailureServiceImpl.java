/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.SystemFailure;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailureType;
import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailuresImpl;
import com.spectralogic.s3.common.dao.service.shared.BaseFailureService;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.ExceptionUtil;

final class SystemFailureServiceImpl 
    extends BaseFailureService< SystemFailure > implements SystemFailureService
{
    SystemFailureServiceImpl()
    {
        super( SystemFailure.class );
    }
    
    
    public void create( 
            final SystemFailureType type,
            final Throwable t,
            final Integer minMinutesSinceLastFailureOfSameType )
    {
        create( 
                type, 
                ExceptionUtil.getReadableMessage( t ),
                minMinutesSinceLastFailureOfSameType );
    }
    
    
    public void create(
            final SystemFailureType type,
            final String error,
            final Integer minMinutesSinceLastFailureOfSameType )
    {
        create( BeanFactory.newBean( SystemFailure.class )
                .setErrorMessage( error )
                .setType( type ), minMinutesSinceLastFailureOfSameType );
    }
    
    
    public void deleteAll( final SystemFailureType type )
    {
        deleteAll( Require.beanPropertyEquals( Failure.TYPE, type ) );
    }
    
    
    public ActiveFailures startActiveFailures( final SystemFailureType type )
    {
        return new ActiveFailuresImpl<>( this, BeanFactory.newBean( SystemFailure.class ).setType( type ) );
    }
}
