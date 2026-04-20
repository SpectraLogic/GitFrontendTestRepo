/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeanRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.exception.GenericFailure;

public final class GetObjectRequestHandler extends BaseGetBeanRequestHandler< S3Object >
{
    public GetObjectRequestHandler()
    {
        super( S3Object.class,
               new BucketAuthorizationStrategy(
                    SystemBucketAccess.STANDARD,
                    BucketAclPermission.LIST, 
                    AdministratorOverride.NO ), 
               RestDomainType.OBJECT );
        registerRequiredBeanProperties( S3Object.BUCKET_ID );
    }
    
    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final S3Object specifiedObject =
                getBeanSpecifiedViaQueryParameters( params, AutoPopulatePropertiesWithDefaults.YES );
        WhereClause identifierFilter = 
                Require.beanPropertyEquals( S3Object.NAME, request.getRestRequest().getIdAsString() );
        try
        {
            identifierFilter = Require.beanPropertyEquals( 
                    Identifiable.ID,
                    UUID.fromString( request.getRestRequest().getIdAsString() ) );
        }
        catch ( final RuntimeException ex )
        {
            LOG.debug( "Identifier is not a UUID.", ex );
        }
        final S3Object retval = params.getServiceManager().getRetriever( S3Object.class ).attain( Require.all(
                Require.beanPropertyEquals( S3Object.BUCKET_ID, specifiedObject.getBucketId() ),
                identifierFilter ) );

        return BeanServlet.serviceGet( params, retval );
    }
}
