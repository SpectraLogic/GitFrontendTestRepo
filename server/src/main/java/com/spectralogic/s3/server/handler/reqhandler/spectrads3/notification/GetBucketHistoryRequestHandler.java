/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.notification;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.notification.BucketHistoryEvent;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.api.RequestParameterValue;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.exception.GenericFailure;

import java.util.List;

public final class GetBucketHistoryRequestHandler extends BaseGetBeansRequestHandler< BucketHistoryEvent >
{
    public GetBucketHistoryRequestHandler()
    {
        super( BucketHistoryEvent.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
               RestDomainType.BUCKET_HISTORY );

        registerOptionalBeanProperties( BucketHistoryEvent.BUCKET_ID );

        registerOptionalRequestParameters( RequestParameterType.MIN_SEQUENCE_NUMBER );
    }
    
    
    @Override
    protected WhereClause getCustomFilter( final BucketHistoryEvent requestBean, final CommandExecutionParams params )
    {
        WhereClause retval = Require.beanPropertyEqualsOneOf(
                BucketHistoryEvent.BUCKET_ID,
                BucketAuthorization.getBucketsUserHasAccessTo(
                        SystemBucketAccess.STANDARD,
                        BucketAclPermission.LIST,
                        AdministratorOverride.YES,
                        params ) );

        if ( params.getRequest().hasRequestParameter( RequestParameterType.MIN_SEQUENCE_NUMBER ) ) {
            final RequestParameterValue minSequenceNumber = params.getRequest().getRequestParameter(RequestParameterType.MIN_SEQUENCE_NUMBER);
            retval = Require.all( retval, Require.beanPropertyGreaterThan( BucketHistoryEvent.SEQUENCE_NUMBER, minSequenceNumber.getLong() ) );
        }
        return retval;
    }


    @Override
    protected List< BucketHistoryEvent > performCustomPopulationWork(
            final DS3Request request,
            final CommandExecutionParams params,
            final List< BucketHistoryEvent > events )
    {
        //We throw exceptions if the specified minimum sequence number does not exist in the database. This is to
        //assure that the user is alerted if the specified sequence number has been aged out, which could mean that
        //others after it are also aged out.
        if ( params.getRequest().hasRequestParameter( RequestParameterType.MIN_SEQUENCE_NUMBER ) ) {
            final long minSequenceNumber = params.getRequest().getRequestParameter(RequestParameterType.MIN_SEQUENCE_NUMBER).getLong();
            final BucketHistoryEvent event = params.getServiceManager().getRetriever( BucketHistoryEvent.class )
                    .retrieve( Require.beanPropertyEquals( BucketHistoryEvent.SEQUENCE_NUMBER, minSequenceNumber ) );
            if ( event == null ) {
                if ( events.isEmpty() )
                {
                    throw new S3RestException( GenericFailure.NOT_FOUND, "No matching events were found that are greater" +
                            " than or equal to " + minSequenceNumber + ". It may be too old, or you may have specified " +
                            "an incorrect bucket filter or not have access to the relevant bucket." );
                }
                else
                {
                    throw new S3RestException( GenericFailure.NOT_FOUND, "No matching event was found for sequence number "
                            + minSequenceNumber + ". The oldest available matching event is "
                            + events.get( 0 ).getSequenceNumber() + "." );
                }
            }
        }
        return events;
    }
}
