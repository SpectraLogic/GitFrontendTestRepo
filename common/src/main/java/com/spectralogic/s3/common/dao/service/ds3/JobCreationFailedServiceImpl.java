package com.spectralogic.s3.common.dao.service.ds3;

/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/

import com.spectralogic.s3.common.dao.domain.ds3.JobCreationFailed;
import com.spectralogic.s3.common.dao.domain.ds3.JobCreationFailedType;
import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailuresImpl;
import com.spectralogic.s3.common.dao.service.shared.BaseFailureService;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;

import java.util.List;

final class JobCreationFailedServiceImpl
        extends BaseFailureService<JobCreationFailed> implements JobCreationFailedService
{
    JobCreationFailedServiceImpl()
    {
        super( JobCreationFailed.class );
    }


    public void create(
            final String userName,
            final JobCreationFailedType type,
            final List<List<String>> tapeBarCodes,
            final String error,
            final Integer minMinutesSinceLastFailureOfSameType )
    {
        create( BeanFactory.newBean( JobCreationFailed.class )
                .setErrorMessage( error )
                .setTapeBarCodes( JobCreationFailedService.barCodesAsString(tapeBarCodes) )
                .setUserName( userName )
                .setType( type ), minMinutesSinceLastFailureOfSameType );
    }


    public void deleteAll( final String userName, final JobCreationFailedType type )
    {
        deleteAll( Require.all(
                Require.beanPropertyEquals( JobCreationFailed.USER_NAME, userName ),
                Require.beanPropertyEquals( Failure.TYPE, type ) ) );
    }


    public void deleteAll( final String userName )
    {
        deleteAll( Require.beanPropertyEquals( JobCreationFailed.USER_NAME, userName ) );
    }


    public ActiveFailures startActiveFailures( final String userName, final JobCreationFailedType type )
    {
        return new ActiveFailuresImpl<>(
                this,
                BeanFactory.newBean( JobCreationFailed.class )
                        .setUserName( userName ).setType( type ) );
    }
}

