/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.servlet.ServletInputStream;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel;
import com.spectralogic.s3.common.dao.orm.BucketRM;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.PreviousVersions;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectFailure;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectFailureReason;
import com.spectralogic.s3.server.domain.DeleteObjectErrorResultApiBean;
import com.spectralogic.s3.server.domain.DeleteResultApiBean;
import com.spectralogic.s3.server.domain.S3ObjectToDeleteApiBean;
import com.spectralogic.s3.server.domain.S3ObjectsToDeleteApiBean;
import com.spectralogic.s3.server.domain.S3ObjectsToDeleteApiBeanSaxHandler;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.BucketRequirement;
import com.spectralogic.s3.server.handler.canhandledeterminer.NonRestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.canhandledeterminer.S3ObjectRequirement;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.marshal.sax.SaxException;
import com.spectralogic.util.marshal.sax.SaxParser;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

public final class DeleteObjectsRequestHandler extends BaseRequestHandler
{
    public DeleteObjectsRequestHandler()
    {
        super( new BucketAuthorizationStrategy(
                SystemBucketAccess.INTERNAL_ONLY,
                BucketAclPermission.DELETE,
                AdministratorOverride.NO ), 
               new NonRestfulCanHandleRequestDeterminer( 
                RequestType.POST,
                BucketRequirement.REQUIRED,
                S3ObjectRequirement.NOT_ALLOWED ) );
        
        registerRequiredRequestParameters( RequestParameterType.DELETE );
    }
    
    
    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final Bucket bucket = params
                .getServiceManager()
                .getRetriever( Bucket.class )
                .attain( Bucket.NAME, request.getBucketName() );
        final S3ObjectsToDeleteApiBean deleteRequest = parseDeleteRequest( params );
    
        if ( EnhancedIterable.MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED
        		< deleteRequest.getObjectsToDelete().size() )
        {
            throw new S3RestException( GenericFailure.BAD_REQUEST, "Cannot request to delete more than " +
                    EnhancedIterable.MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED +
                    " objects in a single request (" + deleteRequest.getObjectsToDelete().size() + ")" );
        }
        
        final boolean strictUnversionedDelete =
                ( null != request.getHttpRequest().getHeader( S3HeaderType.STRICT_UNVERSIONED_DELETE ) );
        final DataPolicy dataPolicy =
                new BucketRM( bucket, params.getServiceManager() ).getDataPolicy().unwrap();
        
        final S3ObjectService objectService = params.getServiceManager().getService( S3ObjectService.class );
        
        final Set< S3Object > objects = new HashSet<>();
        Boolean versionsSpecified = null;
        final Map< S3ObjectToDeleteApiBean, DeleteObjectFailureReason > failedObjects = new HashMap<>();
        for ( final S3ObjectToDeleteApiBean objectToDelete : deleteRequest.getObjectsToDelete() )
        {
        	final Boolean currentDeleteIsVersioned = null != objectToDelete.getVersionId();
        	if ( null == versionsSpecified )
        	{
        		versionsSpecified = currentDeleteIsVersioned;
        	}
        	if ( currentDeleteIsVersioned != versionsSpecified )
        	{
    			throw new S3RestException( GenericFailure.BAD_REQUEST,
    					"Mixing versioned and unversioned deletes in a single request is not supported." );
        	}
        	
        	final boolean unversionedDeletesAffectIncompleteObjects =
            		VersioningLevel.KEEP_MULTIPLE_VERSIONS != dataPolicy.getVersioning();
    		final UUID objectId = objectService.retrieveId(
    				bucket.getName(),
    				objectToDelete.getKey(),
    				objectToDelete.getVersionId(),
    				versionsSpecified || unversionedDeletesAffectIncompleteObjects );
			if ( null == objectId )
			{
				failedObjects.put( objectToDelete, DeleteObjectFailureReason.NOT_FOUND );
			}
			else if ( strictUnversionedDelete && null == objectService.attain( objectId ).getCreationDate() )
			{
				failedObjects.put( objectToDelete, DeleteObjectFailureReason.RETRY_WITH_ASYNCHRONOUS_WAIT );
			}
			else
			{
				objects.add( objectService.attain( objectId ) );
			}
        }
    
        if ( null == versionsSpecified )
        {
            versionsSpecified = false;
        }
        
        final int blobCount = params.getServiceManager()
                                    .getRetriever( Blob.class )
                                    .getCount( Require.beanPropertyEqualsOneOf( Blob.OBJECT_ID, objects.stream()
                                                                                                       .map( Identifiable::getId )
                                                                                                       .collect(
                                                                                                               Collectors.toSet() ) ) );
    
        if ( EnhancedIterable.MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED < blobCount )
        {
            throw new S3RestException( GenericFailure.BAD_REQUEST,
                    "Cannot request to delete objects that have more than" +
                            EnhancedIterable.MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED + " blobs (" +
                            blobCount + ") in a single request" );
        }

        
        final PreviousVersions deleteMode;

        deleteMode = PreviousVersions.determineHandling(
                versionsSpecified,
                dataPolicy.getVersioning(),
                strictUnversionedDelete );
        final DeleteObjectFailure[] failures = params.getTargetResource()
                                                     .deleteObjects(
                request.getAuthorization().getUserId(),
                deleteMode,
                CollectionFactory.toArray( UUID.class, BeanUtils.toMap( objects ).keySet() ) )
                                                     .get( Timeout.LONG )
                                                     .getFailures();

        final DeleteResultApiBean deleteResult = buildDeleteResult(
                versionsSpecified,
                deleteRequest.isQuiet(),
                objects,
                failedObjects,
                failures );
        return BeanServlet.serviceDelete( params, deleteResult );
    }

    private static S3ObjectsToDeleteApiBean parseDeleteRequest( final CommandExecutionParams params )
    {
        final S3ObjectsToDeleteApiBeanSaxHandler deleteRequestSaxHandler =
                new S3ObjectsToDeleteApiBeanSaxHandler();
        final SaxParser saxParser = new SaxParser( deleteRequestSaxHandler );
        saxParser.setInputStream( getRequestInputStream( params ) );
        try
        {
            saxParser.parse();
        }
        catch ( final SaxException ex )
        {
            throw new S3RestException( GenericFailure.BAD_REQUEST, "Invalid XML Response.", ex );
        }
        return deleteRequestSaxHandler.getDeleteRequest();
    }

    
    private static ServletInputStream getRequestInputStream( final CommandExecutionParams params )
    {
        try
        {
            return params.getRequest().getHttpRequest().getInputStream();
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
    }


    private DeleteResultApiBean buildDeleteResult(
            final boolean versionsSpecified,
            boolean quiet,
            final Set< S3Object > objects,
            final Map< S3ObjectToDeleteApiBean, DeleteObjectFailureReason > failedObjects,
            final DeleteObjectFailure[] failures )
    {
        final DeleteResultApiBean deleteResult = BeanFactory.newBean( DeleteResultApiBean.class );
        addFailuresToResult( versionsSpecified, deleteResult, failedObjects, failures, objects );
        if ( !quiet )
        {
            addDeletedObjectsToResult(
                    versionsSpecified, deleteResult, getObjectsWithoutFailures( objects, failures ) );
        }
        return deleteResult;
    }
    

    private static void addFailuresToResult(
    		final boolean versionsSpecified,
            final DeleteResultApiBean deleteResult,
            final Map< S3ObjectToDeleteApiBean, DeleteObjectFailureReason > failedObjects,
            final DeleteObjectFailure[] failures,
            final Set< S3Object > objects )
    {
        final Map< UUID, S3Object > objectsById = BeanUtils.toMap( objects );
        
        final List< DeleteObjectErrorResultApiBean > responseErrors = new ArrayList<>();
        
        for ( final DeleteObjectFailure failure : failures )
        {
            addResponseError(
            		versionsSpecified,
                    responseErrors,
                    objectsById.get( failure.getObjectId() ),
                    failure.getReason() );
        }
        
        for ( final S3ObjectToDeleteApiBean objectNotFound : failedObjects.keySet() )
        {
            addResponseError( 
                    responseErrors, objectNotFound, failedObjects.get( objectNotFound ) );
        }
    
        responseErrors.sort(
                new BeanComparator<>( DeleteObjectErrorResultApiBean.class, DeleteObjectErrorResultApiBean.KEY ) );
        
        deleteResult.setErrors( 
                CollectionFactory.toArray( DeleteObjectErrorResultApiBean.class, responseErrors ) );
    }

    
    private static void addResponseError(
            final List< DeleteObjectErrorResultApiBean > responseErrors,
            final S3ObjectToDeleteApiBean object,
            final DeleteObjectFailureReason reason )
    {
        final DeleteObjectErrorResultApiBean responseError = 
                BeanFactory.newBean( DeleteObjectErrorResultApiBean.class );
        responseError.setKey( object.getKey() );
        responseError.setVersionId( object.getVersionId() );
        setReason( responseError, reason );
        responseErrors.add( responseError );
    }
    
    
    private static void addResponseError(
    		final boolean versionsSpecified,
            final List< DeleteObjectErrorResultApiBean > responseErrors,
            final S3Object object,
            final DeleteObjectFailureReason reason )
    {
        final DeleteObjectErrorResultApiBean responseError = 
                BeanFactory.newBean( DeleteObjectErrorResultApiBean.class );
        responseError.setKey( object.getName() );
        if ( versionsSpecified )
        {
        	responseError.setVersionId( object.getId() );
        }
        setReason( responseError, reason );
        responseErrors.add( responseError );
    }

    
    private static void setReason(
    		final DeleteObjectErrorResultApiBean responseError,
    		DeleteObjectFailureReason reason )
    {
    	switch ( reason )
	    {
	        case NOT_FOUND:
	            responseError.setCode( "ObjectNotFound" );
	            responseError.setMessage( "Object not found" );
	            break;
	        case RETRY_WITH_ASYNCHRONOUS_WAIT:
	        	responseError.setCode( "RetryWithAsynchronousWait");
	        	responseError.setMessage( "Versioned object has not yet arrived in cache.");
	        	break;
	        
	        default:
	            throw new UnsupportedOperationException( "No code to handle: " + reason );
	    }
    }
    
    
    private static void addDeletedObjectsToResult(
            final boolean versionsSpecified,
            final DeleteResultApiBean deleteResult,
            final Collection< S3Object > objectsWithoutErrors )
    {
        final List< S3ObjectToDeleteApiBean > deleteResults = new ArrayList<>();
        for ( final S3Object deletedObject : objectsWithoutErrors )
        {
            final S3ObjectToDeleteApiBean deletedObjectResult = 
                    BeanFactory.newBean( S3ObjectToDeleteApiBean.class );
            deletedObjectResult.setKey( deletedObject.getName() );
            if ( versionsSpecified )
            {
            	deletedObjectResult.setVersionId( deletedObject.getId() );	
            }
            deleteResults.add( deletedObjectResult );
        }
    
        deleteResults.sort( new BeanComparator<>( S3ObjectToDeleteApiBean.class, S3ObjectToDeleteApiBean.KEY ) );
        
        deleteResult.setDeletedObjects( 
                CollectionFactory.toArray( S3ObjectToDeleteApiBean.class, deleteResults ) );
    }
    
    
    private static Collection< S3Object > getObjectsWithoutFailures(
            final Collection< S3Object > objects,
            final DeleteObjectFailure[] failures )
    {
        final Set< UUID > objectIdsWithFailures = getObjectIdsFromFailures( failures );
        final Collection< S3Object > result = new ArrayList<>();
        for ( final S3Object object : objects )
        {
            if ( !objectIdsWithFailures.contains( object.getId() ) )
            {
                result.add( object );
            }
        }
        return result;
    }


    private static Set< UUID > getObjectIdsFromFailures( final DeleteObjectFailure[] errors )
    {
        final Set< UUID > objectIds = new HashSet<>();
        for ( final DeleteObjectFailure error : errors )
        {
            objectIds.add( error.getObjectId() );
        }
        return objectIds;
    }
}
