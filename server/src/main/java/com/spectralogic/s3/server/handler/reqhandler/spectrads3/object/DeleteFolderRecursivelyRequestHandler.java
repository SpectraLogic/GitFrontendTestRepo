/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel;
import com.spectralogic.s3.common.dao.orm.S3ObjectRM;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.PreviousVersions;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.Sanitize;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class DeleteFolderRecursivelyRequestHandler extends BaseRequestHandler
{
    public DeleteFolderRecursivelyRequestHandler()
    {
        super( new BucketAuthorizationStrategy(
                SystemBucketAccess.INTERNAL_ONLY, 
                BucketAclPermission.DELETE, 
                AdministratorOverride.NO ),
               new RestfulCanHandleRequestDeterminer(
                RestActionType.DELETE,
                RestDomainType.FOLDER ) );
        
        registerRequiredRequestParameters( RequestParameterType.RECURSIVE );
        registerRequiredRequestParameters( RequestParameterType.BUCKET_ID );
    }

    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final String folderPrefix = Sanitize.patternLiteral( request.getRestRequest()
                                                                    .getIdAsString() ) + S3Object.DELIMITER + "%";
        final String bucketSpecifier = request.getRequestParameter( RequestParameterType.BUCKET_ID )
                                              .getString();
        Bucket bucket = params.getServiceManager()
                              .getRetriever( Bucket.class )
                              .retrieve( Require.all( Require.beanPropertyEquals( Bucket.NAME, bucketSpecifier ) ) );
    
        if ( null == bucket )
        {
            bucket = params.getServiceManager()
                           .getRetriever( Bucket.class )
                           .retrieve( Require.all(
                                   Require.beanPropertyEquals( Bucket.ID, UUID.fromString( bucketSpecifier ) ) ) );
            if ( null == bucket )
            {
                throw new S3RestException( GenericFailure.NOT_FOUND,
                        "There is no bucket found identified by " + bucketSpecifier + "." );
            }
        }
    
        final S3Object folder = params.getServiceManager()
                                      .getRetriever( S3Object.class )
                                      .retrieveAll( Require.all(
                                              Require.beanPropertyEquals( S3Object.BUCKET_ID, bucket.getId() ),
                                              Require.beanPropertyMatches( S3Object.NAME, folderPrefix ) ) )
                                      .getFirst();
    
        if ( null == folder )
        {
            throw new S3RestException( GenericFailure.NOT_FOUND,
                    "There is no folder in bucket " + bucket.getName() + " with name " + folderPrefix + "." );
        }
    
        final DataPolicy dataPolicy = new S3ObjectRM( folder, params.getServiceManager() ).getBucket()
                                                                                          .getDataPolicy()
                                                                                          .unwrap();
    
        params.getTargetResource()
              .deleteObjects(
                request.getAuthorization().getUserId(),
                      PreviousVersions.determineHandling( false, dataPolicy.getVersioning() ),
                      CollectionFactory.toArray( UUID.class, extractObjectIds( params.getServiceManager()
                                                                                     .getRetriever( S3Object.class ),
                              bucket.getId(), folderPrefix,
                                        VersioningLevel.KEEP_MULTIPLE_VERSIONS == dataPolicy.getVersioning() ) ) )
                .get( Timeout.LONG );
        
        return BeanServlet.serviceDelete( params, null );
    }
    
    
    private Set< UUID > extractObjectIds( final BeansRetriever< S3Object > retriever, final UUID bucketId,
            final String folderPrefix, final boolean requireLatest )
    {
    	final Set< UUID > retval = new HashSet<>();
    	final WhereClause latestFilter =
                requireLatest ? Require.beanPropertyEquals( S3Object.LATEST, Boolean.TRUE ) : Require.nothing();
        try ( final EnhancedIterable< S3Object > iterable = retriever.retrieveAll(
                Require.all( latestFilter, Require.beanPropertyEquals( S3Object.BUCKET_ID, bucketId ),
                        Require.beanPropertyMatches( S3Object.NAME, folderPrefix ) ) )
                                                                     .toIterable() )
        {
            for ( final S3Object o : iterable )
            {
                retval.add( o.getId() );
            }
        }
        
        return retval;
    }
}
