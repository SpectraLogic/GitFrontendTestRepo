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
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobDs3Target;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;

public class GetSuspectBucketsRequestHandler extends BaseGetBeansRequestHandler< Bucket >
{
    public GetSuspectBucketsRequestHandler()
    {
        super( Bucket.class,
                new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ), 
                RestDomainType.SUSPECT_BUCKET );
         
        registerOptionalBeanProperties(
                NameObservable.NAME,
                Bucket.DATA_POLICY_ID,
                UserIdObservable.USER_ID );
     }
     
     
     @Override
     protected WhereClause getCustomFilter( final Bucket requestBean, final CommandExecutionParams params )
     {
         final ArrayList< WhereClause > suspectLocations = new ArrayList<>();
         final Map< Class< ? extends DatabasePersistable >, WhereClause > whereClauseMap = new HashMap<>();
    
         whereClauseMap.put( SuspectBlobTape.class,
                 Require.exists( SuspectBlobTape.class, BlobObservable.BLOB_ID, Require.nothing() ) );
         whereClauseMap.put( SuspectBlobPool.class,
                 Require.exists( SuspectBlobPool.class, BlobObservable.BLOB_ID, Require.nothing() ) );
         whereClauseMap.put( SuspectBlobDs3Target.class,
                 Require.exists( SuspectBlobDs3Target.class, BlobObservable.BLOB_ID, Require.nothing() ) );
    
         whereClauseMap.keySet()
                       .forEach( suspectPersistable -> {
                           if ( 0 < params.getServiceManager()
                                          .getRetriever( suspectPersistable )
                                          .getCount() )
                           {
                               suspectLocations.add( whereClauseMap.get( suspectPersistable ) );
                           }
                       } );
    
         final WhereClause suspectFilter = Require.exists( S3Object.class, S3Object.BUCKET_ID,
                 Require.exists( Blob.class, Blob.OBJECT_ID, Require.any( suspectLocations ) ) );
    
         final WhereClause authorizationFilter = Require.beanPropertyEqualsOneOf( Identifiable.ID,
                 BucketAuthorization.getBucketsUserHasAccessTo( SystemBucketAccess.STANDARD, BucketAclPermission.LIST,
                         AdministratorOverride.YES, params ) );
    
         return Require.all( suspectFilter, authorizationFilter );
     }
}
