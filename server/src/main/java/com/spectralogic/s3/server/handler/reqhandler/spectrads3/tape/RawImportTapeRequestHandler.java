/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.shared.ImportPersistenceTargetDirective;
import com.spectralogic.s3.common.dao.domain.tape.RawImportTapeDirective;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.orm.BucketRM;
import com.spectralogic.s3.common.rpc.dataplanner.domain.ImportPersistenceTargetDirectiveRequest;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class RawImportTapeRequestHandler extends BaseDaoTypedRequestHandler< RawImportTapeDirective >
{
    public RawImportTapeRequestHandler()
    {
        super( RawImportTapeDirective.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.ADMINISTRATOR ),
               new RestfulCanHandleRequestDeterminer(
                       RestOperationType.IMPORT,
                       RestDomainType.TAPE ) );
        registerOptionalRequestParameters( 
                RequestParameterType.TASK_PRIORITY );
        registerRequiredBeanProperties( 
                RawImportTapeDirective.BUCKET_ID );
        registerOptionalBeanProperties(
                ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Tape tape = 
                request.getRestRequest().getBean( params.getServiceManager().getRetriever( Tape.class ) );
        final RawImportTapeDirective importDirective = 
                getBeanSpecifiedViaQueryParameters( params, AutoPopulatePropertiesWithDefaults.YES );
        final BucketRM bucket = new BucketRM( importDirective.getBucketId(), params.getServiceManager() );
        importDirective.setTapeId( tape.getId() );

        BlobStoreTaskPriority priority = BlobStoreTaskPriority.NORMAL;
        if ( request.hasRequestParameter( RequestParameterType.TASK_PRIORITY ) )
        {
            priority = request.getRequestParameter( RequestParameterType.TASK_PRIORITY ).getEnum(
                    BlobStoreTaskPriority.class );
        }
        params.getTapeResource().rawImportTape( 
                tape.getId(),
                importDirective.getBucketId(),
                BeanFactory.newBean( ImportPersistenceTargetDirectiveRequest.class )
	                .setDataPolicyId( bucket.unwrap().getDataPolicyId() )
	                .setStorageDomainId( importDirective.getStorageDomainId() )
	                .setUserId( importDirective.getUserId() )
	                .setPriority( priority ) ).get( Timeout.DEFAULT );
        
        return BeanServlet.serviceModify(
                params, 
                params.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ) );
    }
}
