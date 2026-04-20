/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainService;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.handler.reqhandler.spectrads3.object.PhysicalPlacementCalculator;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;

public final class GetSuspectObjectsWithFullDetailsRequestHandler extends BaseDaoTypedRequestHandler< Tape >
{
    public GetSuspectObjectsWithFullDetailsRequestHandler()
    {
        super( Tape.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
                new RestfulCanHandleRequestDeterminer(
                        RestActionType.LIST,
                        RestDomainType.SUSPECT_OBJECT ) );

        registerOptionalRequestParameters( RequestParameterType.STORAGE_DOMAIN );
        
        registerOptionalBeanProperties( PersistenceTarget.BUCKET_ID );
        registerRequiredRequestParameters( RequestParameterType.FULL_DETAILS );
    }


    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Tape pt = getBeanSpecifiedViaQueryParameters( 
                params, AutoPopulatePropertiesWithDefaults.YES );
        if ( null == pt.getBucketId() )
        {
            new DefaultPublicExposureAuthenticationStrategy( 
                    RequiredAuthentication.ADMINISTRATOR ).authenticate( params );
        }
        else
        {
            BucketAuthorization.verify(
                    SystemBucketAccess.STANDARD, 
                    BucketAclPermission.LIST, 
                    AdministratorOverride.YES, 
                    params,
                    pt.getBucketId() );
        }
        final Set< Blob > blobs = params.getServiceManager().getRetriever( Blob.class ).retrieveAll( 
                Require.all( GetSuspectObjectsRequestHandler.getSuspectBlobFilter( params ),
                        ( null == pt.getBucketId() ) ?
                                null
                                : Require.exists( 
                                        Blob.OBJECT_ID,
                                        Require.beanPropertyEquals( 
                                                S3Object.BUCKET_ID, pt.getBucketId() ) ) ) ).toSet();

        final Set< UUID > blobIds = BeanUtils.toMap( blobs ).keySet();
        blobs.clear(); // free up the RAM

        final UUID storageDomainId;
        if (request.hasRequestParameter( RequestParameterType.STORAGE_DOMAIN ) )
        {
            storageDomainId = params.getServiceManager().getService( StorageDomainService.class ).attain(
                    request.getRequestParameter( RequestParameterType.STORAGE_DOMAIN ).getString() ).getId();
        }
        else
        {
            storageDomainId = null;
        }
        return BeanServlet.serviceGet( 
                params, 
                new PhysicalPlacementCalculator( 
                        storageDomainId,
                        params, 
                        false, 
                        ( null == pt.getBucketId() ) ? 
                                null 
                                : params.getServiceManager().getRetriever( Bucket.class ).attain(
                                        pt.getBucketId() ),
                        blobIds,
                        request.hasRequestParameter( RequestParameterType.FULL_DETAILS ),
                        true ).getResult() );
    }
}
