/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectPropertyService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.domain.BlobApiBeansContainer;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobsInCacheInformation;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy;
import com.spectralogic.s3.server.handler.auth.DefaultPublicExposureAuthenticationStrategy.RequiredAuthentication;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseGetBeansRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.marshal.CustomMarshaledTypeName;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

abstract class BaseGetObjectsRequestHandler extends BaseGetBeansRequestHandler< S3Object >
{
    protected BaseGetObjectsRequestHandler()
    {
        super( S3Object.class,
               new DefaultPublicExposureAuthenticationStrategy( RequiredAuthentication.USER ),
               RestDomainType.OBJECT );
        
        registerOptionalRequestParameters(
        		RequestParameterType.START_DATE,
        		RequestParameterType.END_DATE );
        
        registerOptionalBeanProperties( 
                NameObservable.NAME,
                S3Object.BUCKET_ID,
                S3Object.TYPE,
                S3Object.LATEST );
    }
    
    
    @Override
    protected WhereClause getCustomFilter( final S3Object requestBean, final CommandExecutionParams params )
    {
    	final DS3Request request = params.getRequest();
    	
        if ( null == request.getAuthorization().getUser() )
        {
            return null;
        }
        WhereClause filter = Require.beanPropertyEqualsOneOf(
							                S3Object.BUCKET_ID, 
							                BucketAuthorization.getBucketsUserHasAccessTo(
							                        SystemBucketAccess.STANDARD,
							                        BucketAclPermission.LIST, 
							                        AdministratorOverride.NO, 
							                        params ) );
        if ( params.getRequest().hasRequestParameter( RequestParameterType.START_DATE ) )
        {
        	
        	filter = Require.all( filter, Require.beanPropertyGreaterThan(
        			S3Object.CREATION_DATE,
    				new Date( request.getRequestParameter( RequestParameterType.START_DATE ).getLong() ) ) );
        }
        if ( params.getRequest().hasRequestParameter( RequestParameterType.END_DATE ) )
        {
        	filter = Require.all( filter, Require.beanPropertyLessThan(
        			S3Object.CREATION_DATE,
    				new Date( request.getRequestParameter( RequestParameterType.END_DATE ).getLong() ) ) );
        }
        return filter;
    }


    @Override
    protected List< ? extends S3Object > performCustomPopulationWork(
            final DS3Request request,
            final CommandExecutionParams params,
            List< S3Object > beans )
    {
        if ( !request.hasRequestParameter( RequestParameterType.FULL_DETAILS ) )
        {
            return beans;
        }
        
        final Map< UUID, Bucket > buckets = 
                BeanUtils.toMap( params.getServiceManager().getRetriever( Bucket.class ).retrieveAll( 
                        BeanUtils.< UUID >extractPropertyValues( beans, S3Object.BUCKET_ID ) ).toSet() );
        final Map< UUID, User > users = 
                BeanUtils.toMap( params.getServiceManager().getRetriever( User.class ).retrieveAll( 
                        BeanUtils.< UUID >extractPropertyValues( 
                                buckets.values(), UserIdObservable.USER_ID ) ).toSet() );
        
        final List< DetailedS3Object > retval = new ArrayList<>();
        for ( final S3Object o : beans )
        {
            final DetailedS3Object d = BeanFactory.newBean( DetailedS3Object.class );
            BeanCopier.copy( d, o );
            if ( S3ObjectType.DATA == o.getType() )
            {
                final S3ObjectProperty etag =
                        params.getServiceManager().getService( S3ObjectPropertyService.class ).retrieve(
                                Require.all( 
                                        Require.beanPropertyEquals( 
                                                S3ObjectProperty.OBJECT_ID, 
                                                o.getId() ),
                                        Require.beanPropertyEquals( 
                                                KeyValueObservable.KEY, 
                                                S3HeaderType.ETAG.getHttpHeaderName() ) ) );
                d.setETag( ( null == etag ) ? null : etag.getValue() );
                d.setSize( params.getServiceManager().getService( S3ObjectService.class ).getSizeInBytes( 
                        CollectionFactory.toSet( o.getId() ) ) );
                d.setOwner( users.get( buckets.get( o.getBucketId() ).getUserId() ).getName() );
            }
            
            if ( request.hasRequestParameter( RequestParameterType.INCLUDE_PHYSICAL_PLACEMENT ) )
            {
                final Set< UUID > blobIds =
                        BeanUtils.toMap( params.getServiceManager().getRetriever( Blob.class ).retrieveAll(
                                Blob.OBJECT_ID, o.getId() ).toSet() ).keySet();
                final BlobApiBeansContainer pp = (BlobApiBeansContainer)new PhysicalPlacementCalculator(
                        null,
                        params,
                        false, 
                        buckets.get( o.getBucketId() ),
                        blobIds,
                        true,
                        false ).getResult();
                final BlobsInCacheInformation bic = params.getPlannerResource().getBlobsInCache( 
                        CollectionFactory.toArray( UUID.class, blobIds ) ).get( Timeout.DEFAULT );
                d.setBlobsInCache( Integer.valueOf( bic.getBlobsInCache().length ) );
                d.setBlobsTotal( Integer.valueOf( blobIds.size() ) );
                d.setBlobsBeingPersisted( Integer.valueOf( params.getServiceManager().getRetriever(
                        JobEntry.class ).getCount( Require.all(
                                Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ),
                                Require.exists(
                                        JobEntry.JOB_ID,
                                        Require.beanPropertyEquals(
                                                JobObservable.REQUEST_TYPE, JobRequestType.PUT ) ) ) ) ) );
                d.setBlobsDegraded( Integer.valueOf( BeanUtils.extractPropertyValues( 
                        params.getServiceManager().getRetriever( DegradedBlob.class ).retrieveAll( 
                                Require.beanPropertyEqualsOneOf( BlobObservable.BLOB_ID, blobIds ) ).toSet(),
                        BlobObservable.BLOB_ID ).size() ) );
                d.setBlobs( pp );
            }
            
            retval.add( d );
        }
        
        return retval;
    }
    
    
    @CustomMarshaledTypeName( "Object" )
    interface DetailedS3Object extends S3Object, SimpleBeanSafeToProxy
    {
        String E_TAG = "eTag";
        
        String getETag();
        
        void setETag( final String value );
        
        
        String SIZE = "size";
        
        long getSize();
        
        void setSize( final long value );
        
        
        String OWNER = "owner";
        
        String getOwner();
        
        void setOwner( final String value );
        
        
        String BLOBS_IN_CACHE = "blobsInCache";

        @ExcludeFromMarshaler( When.VALUE_IS_NULL )
        Integer getBlobsInCache();
        
        void setBlobsInCache( final Integer value );
        
        
        String BLOBS_TOTAL = "blobsTotal";

        @ExcludeFromMarshaler( When.VALUE_IS_NULL )
        Integer getBlobsTotal();
        
        void setBlobsTotal( final Integer value );
        
        
        String BLOBS_DEGRADED = "blobsDegraded";

        @ExcludeFromMarshaler( When.VALUE_IS_NULL )
        Integer getBlobsDegraded();
        
        void setBlobsDegraded( final Integer value );
        
        
        String BLOBS_BEING_PERSISTED = "blobsBeingPersisted";

        @ExcludeFromMarshaler( When.VALUE_IS_NULL )
        Integer getBlobsBeingPersisted();
        
        void setBlobsBeingPersisted( final Integer value );
        

        String BLOBS = "blobs";
        
        @ExcludeFromMarshaler( When.VALUE_IS_NULL )
        BlobApiBeansContainer getBlobs();
        
        void setBlobs( final BlobApiBeansContainer physicalPlacement );
    } // end inner class def
}
