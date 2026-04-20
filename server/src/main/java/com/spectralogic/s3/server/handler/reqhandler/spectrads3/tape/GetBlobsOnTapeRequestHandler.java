/*
 *
 * Copyright C 2017., Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.platform.domain.BlobApiBeanBuilder;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.RequestPagingProperties;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.RequestSortingProperties;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import static com.spectralogic.s3.server.handler.reqhandler.frmwk.FillResultSetUtil.fillResultSet;

public final class GetBlobsOnTapeRequestHandler extends BaseRequestHandler
{
    public GetBlobsOnTapeRequestHandler()
    {
        super( new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.TAPE_ADMIN ),
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.SHOW, 
                       RestOperationType.GET_PHYSICAL_PLACEMENT,
                       RestDomainType.TAPE ) );
    
        registerOptionalRequestParameters( RequestParameterType.PAGE_LENGTH, RequestParameterType.PAGE_OFFSET,
                RequestParameterType.LAST_PAGE, RequestParameterType.PAGE_START_MARKER );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final BeansRetrieverManager brm = params.getServiceManager();
        final Tape specifiedTape = request.getRestRequest().getBean(
                brm.getRetriever( Tape.class ) );
        final WhereClause whereClause = Require.exists( BlobTape.class, BlobObservable.BLOB_ID,
                Require.beanPropertyEquals( BlobTape.TAPE_ID, specifiedTape.getId() ) );
        final BeansRetriever< Blob > retriever = brm.getRetriever( Blob.class );
        final RequestPagingProperties pagingProperties =
                new RequestPagingProperties( request, retriever.getCount( whereClause ) );
    
        final Set< Blob > resultSet = new HashSet<>();
        fillResultSet( Collections.singletonList( whereClause ), retriever, pagingProperties,
                new RequestSortingProperties(), resultSet );
    
        return BeanServlet.serviceGet(
                params, new BlobApiBeanBuilder( brm.getRetriever( Bucket.class ), brm.getRetriever( S3Object.class ),
                        resultSet ).buildAndWrap() );
    }
}
