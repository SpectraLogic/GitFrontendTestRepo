/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.s3;

import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.platform.domain.BlobApiBeanBuilder;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;

public final class GetBlobsOnS3TargetRequestHandler extends BaseRequestHandler
{
    public GetBlobsOnS3TargetRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.SHOW, 
                       RestOperationType.GET_PHYSICAL_PLACEMENT,
                       RestDomainType.S3_TARGET ) );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final BeansRetrieverManager brm = params.getServiceManager();
        final S3Target specifiedS3Target = request.getRestRequest().getBean(
                brm.getRetriever( S3Target.class ) );
        final Set< Blob > blobs = brm.getRetriever( Blob.class ).retrieveAll( Require.exists( 
                BlobS3Target.class, 
                BlobObservable.BLOB_ID, 
                Require.beanPropertyEquals( 
                        BlobTarget.TARGET_ID, specifiedS3Target.getId() ) ) ).toSet();
        return BeanServlet.serviceGet(
                params, 
                new BlobApiBeanBuilder( 
                        brm.getRetriever( Bucket.class ), 
                        brm.getRetriever( S3Object.class ),
                        blobs ).buildAndWrap() );
    }
}
