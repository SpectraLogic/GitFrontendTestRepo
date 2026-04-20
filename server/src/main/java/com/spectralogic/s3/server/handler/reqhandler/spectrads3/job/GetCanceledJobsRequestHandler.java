/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.CanceledJob;
import com.spectralogic.s3.common.dao.domain.ds3.JobObservable;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;

public final class GetCanceledJobsRequestHandler extends BaseGetBeansRequestHandler< CanceledJob >
{
    public GetCanceledJobsRequestHandler()
    {
        super( CanceledJob.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.CANCELED_JOB );
        
        registerOptionalBeanProperties( 
                NameObservable.NAME,
                JobObservable.BUCKET_ID, 
                JobObservable.CHUNK_CLIENT_PROCESSING_ORDER_GUARANTEE, 
                JobObservable.PRIORITY,
                JobObservable.RECHUNKED, 
                JobObservable.REQUEST_TYPE, 
                JobObservable.TRUNCATED,
                UserIdObservable.USER_ID,
                CanceledJob.CANCELED_DUE_TO_TIMEOUT );
    }

    
    @Override
    protected WhereClause getCustomFilter(
            final CanceledJob requestBean,
            final CommandExecutionParams params )
    {
        return Require.beanPropertyEqualsOneOf( 
                JobObservable.BUCKET_ID, 
                BucketAuthorization.getBucketsUserHasAccessTo(
                        SystemBucketAccess.STANDARD, 
                        BucketAclPermission.JOB, 
                        AdministratorOverride.YES,
                        params ) );
    }
}
