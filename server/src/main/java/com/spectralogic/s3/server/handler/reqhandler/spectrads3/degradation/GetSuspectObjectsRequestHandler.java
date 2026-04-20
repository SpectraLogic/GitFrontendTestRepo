/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobDs3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;


public class GetSuspectObjectsRequestHandler extends BaseGetBeansRequestHandler< S3Object >
{
    public GetSuspectObjectsRequestHandler()
    {
        super( S3Object.class,
                new BucketAuthorizationStrategy(
                        SystemBucketAccess.STANDARD, BucketAclPermission.LIST, AdministratorOverride.YES ),
                RestDomainType.SUSPECT_OBJECT );

        registerOptionalBeanProperties( S3Object.BUCKET_ID );
    }

    
    static WhereClause getSuspectBlobFilter( final CommandExecutionParams params )
    {
        final ArrayList< WhereClause > suspectLocations = new ArrayList<>();
        final Map< Class< ? extends DatabasePersistable >, WhereClause > whereClauseMap = new HashMap<>();
    
        whereClauseMap.put( SuspectBlobTape.class,
                Require.exists( SuspectBlobTape.class, BlobObservable.BLOB_ID, Require.nothing() ) );
        whereClauseMap.put( SuspectBlobPool.class,
                Require.exists( SuspectBlobPool.class, BlobObservable.BLOB_ID, Require.nothing() ) );
        whereClauseMap.put( SuspectBlobDs3Target.class,
                Require.exists( SuspectBlobDs3Target.class, BlobObservable.BLOB_ID, Require.nothing() ) );
        whereClauseMap.put( SuspectBlobAzureTarget.class,
                Require.exists( SuspectBlobAzureTarget.class, BlobObservable.BLOB_ID, Require.nothing() ) );
        whereClauseMap.put( SuspectBlobS3Target.class,
                Require.exists( SuspectBlobS3Target.class, BlobObservable.BLOB_ID, Require.nothing() ) );
    
        whereClauseMap.keySet()
                      .forEach( suspectPersistable -> {
                          if ( 0 < params.getServiceManager()
                                         .getRetriever( suspectPersistable )
                                         .getCount() )
                          {
                              suspectLocations.add( whereClauseMap.get( suspectPersistable ) );
                          }
                      } );
    
        return Require.any( suspectLocations );
    }
    
    
    @Override
    protected WhereClause getCustomFilter( final S3Object object, final CommandExecutionParams params )
    {
        return Require.exists(
                Blob.class,
                Blob.OBJECT_ID, getSuspectBlobFilter( params ) );
    }
}
