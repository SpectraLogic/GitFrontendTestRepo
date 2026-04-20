/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.amazons3;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectProperty;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectType;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectPropertyService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.security.BucketAclAuthorizationService.AdministratorOverride;
import com.spectralogic.s3.server.domain.BucketObjectsApiBean;
import com.spectralogic.s3.server.domain.S3ObjectApiBean;
import com.spectralogic.s3.server.domain.UserApiBean;
import com.spectralogic.s3.server.domain.UserApiBeanImpl;
import com.spectralogic.s3.server.handler.auth.BucketAuthorization.SystemBucketAccess;
import com.spectralogic.s3.server.handler.auth.BucketAuthorizationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.BucketRequirement;
import com.spectralogic.s3.server.handler.canhandledeterminer.NonRestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.S3ObjectRequirement;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.handler.reqhandler.frmwk.BaseDaoTypedRequestHandler;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.Sanitize;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;

public final class GetBucketRequestHandler extends BaseDaoTypedRequestHandler< BucketObjectsApiBean >
{
    public GetBucketRequestHandler()
    {
        super( BucketObjectsApiBean.class,
               new BucketAuthorizationStrategy(
                SystemBucketAccess.STANDARD,
                BucketAclPermission.LIST,
                AdministratorOverride.YES ), 
               new NonRestfulCanHandleRequestDeterminer( 
                RequestType.GET,
                BucketRequirement.REQUIRED,
                S3ObjectRequirement.NOT_ALLOWED ) );
        
        registerOptionalBeanProperties( 
                BucketObjectsApiBean.PREFIX,
                BucketObjectsApiBean.DELIMITER, 
                BucketObjectsApiBean.MARKER, 
                BucketObjectsApiBean.MAX_KEYS );
                
        registerOptionalRequestParameters(
        	RequestParameterType.VERSIONS );
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request,
            final CommandExecutionParams params )
    {
        final BeansServiceManager serviceManager = params.getServiceManager();

        final Bucket bucket = serviceManager
                .getRetriever( Bucket.class )
                .attain( Bucket.NAME, request.getBucketName() );
        
        final BucketObjectsApiBean retval = getBeanSpecifiedViaQueryParameters(
                params, AutoPopulatePropertiesWithDefaults.YES );
        if ( 0 >= retval.getMaxKeys() )
        {
            retval.setMaxKeys( BucketObjectsApiBean.DEFAULT_MAX_KEYS );
        }
        retval.setName( bucket.getName() );
        retval.setCreationDate( bucket.getCreationDate() );
        
        final User bucketUser =
                serviceManager.getRetriever( User.class ).attain( bucket.getUserId() );
        final UserApiBean userResult = UserApiBeanImpl.fromUser( bucketUser );

        final S3ObjectService objectService = serviceManager.getService( S3ObjectService.class );
        final ResultPage results = listObjectsInBucket(
                objectService,
                bucket.getId(),
                retval.getPrefix(),
                retval.getDelimiter(),
                retval.getMarker(),
                retval.getMaxKeys(),
                request.hasRequestParameter( RequestParameterType.VERSIONS ) );
        
        final boolean versions = request.hasRequestParameter( RequestParameterType.VERSIONS );
        retval.setNextMarker( results.getNextMarker() );
        retval.setTruncated( results.getNextMarker() != null );
        final S3ObjectApiBean[] objects = CollectionFactory.toArray( S3ObjectApiBean.class, createResponseObjects(
                objectService,
                serviceManager.getService( S3ObjectPropertyService.class ),
                userResult,
                results.getObjects(),
                versions ) ) ;
        
        if ( versions )
        {
        	retval.setVersionedObjects( objects );
        }
        else
        {
        	retval.setObjects( objects );
        }
        
        retval.setCommonPrefixes( CollectionFactory.toArray( String.class, results.m_prefixes ) );
        
        return BeanServlet.serviceGet( params, retval );
    }
    
    
    private static List< S3ObjectApiBean > createResponseObjects(
            final S3ObjectService objectService,
            final S3ObjectPropertyService objectPropertyService,
            final UserApiBean owner,
            final List< S3Object > objects,
            final boolean versions )
    {
        final Map< UUID, String > objectIdToETagMapping =
                getObjectIdToETagMapping( objectPropertyService, BeanUtils.toMap( objects ).keySet() );

        final List< S3ObjectApiBean > resultObjects = new ArrayList<>();
        for ( final S3Object object : objects )
        {
            final S3ObjectApiBean resultObject = BeanFactory.newBean( S3ObjectApiBean.class );
            resultObject.setKey( object.getName() );
            resultObject.setLastModified( object.getCreationDate() );
            resultObject.setETag( objectIdToETagMapping.get( object.getId() ) );
            if ( resultObject.getETag() == null && object.getType() != S3ObjectType.FOLDER )
            {
                LOG.debug( "Object " + object.getId() + " is missing header "
                        + S3HeaderType.ETAG + ".  The object was likely created "
                        + "as part of a PUT job, but has not been completely uploaded yet. " );
            }
            resultObject.setSize( objectService.getSizeInBytes( CollectionFactory.toSet( object.getId() ) ) );
            resultObject.setOwner( owner );
            if ( versions )
            {
            	resultObject.setVersionId( object.getId() );
            	resultObject.setIsLatest( object.isLatest() );
            }
            resultObjects.add( resultObject );
        }
        return resultObjects;
    }
    
    
    private static Map< UUID, String > getObjectIdToETagMapping(
            final BeansRetriever< S3ObjectProperty > objectPropertyService,
            final Set< UUID > objectIds )
    {
        final Set< S3ObjectProperty > eTagProperties = objectPropertyService
                .retrieveAll( Require.all(
                        Require.beanPropertyEqualsOneOf( S3ObjectProperty.OBJECT_ID, objectIds ),
                        Require.beanPropertyEquals(
                                KeyValueObservable.KEY,
                                S3HeaderType.ETAG.getHttpHeaderName() ) ) )
                .toSet();
        final Map< UUID, String > mapping = new HashMap<>();
        for ( final S3ObjectProperty property : eTagProperties )
        {
            mapping.put( property.getObjectId(), property.getValue() );
        } 
        return mapping;
    }
    
    
    static ResultPage listObjectsInBucket(
            final BeansRetriever< S3Object > objectService,
            final UUID bucketId,
            final String prefix,
            final String delimiter,
            final String marker,
            final int maxKeys,
            final boolean versions )
    {
        final Collection< WhereClause > clauses = new ArrayList<>();
        clauses.add( Require.beanPropertyEquals( S3Object.BUCKET_ID, bucketId ) );
        if ( versions )
        {
        	clauses.add( Require.not( Require.beanPropertyEquals( S3Object.CREATION_DATE, null ) ) );
        }
        else
        {
        	clauses.add( Require.beanPropertyEquals( S3Object.LATEST, Boolean.TRUE ) );
        }
        if ( prefix != null )
        {
            clauses.add( Require.beanPropertyMatches(
                    S3Object.NAME,
                    Sanitize.patternLiteral( new StringBuilder(), prefix ).append( '%' ).toString() ) );
        }
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( S3Object.BUCKET_ID, SortBy.Direction.ASCENDING );
        ordering.add( S3Object.NAME, SortBy.Direction.ASCENDING );

        final ResultPage resultPage = new ResultPage();
        resultPage.setNextMarker(marker);
        List<S3Object> objects;
        do {
            final List< WhereClause > batchClauses = new ArrayList<>( clauses );
            if ( resultPage.getNextMarker() != null ) {
                batchClauses.add( Require.beanPropertyGreaterThan( S3Object.NAME, resultPage.getNextMarker() ) );
                //If our next marker is something that could be a common prefix rather than a specific object,
                //then we don't want to examine subresults. For example, if we already found /foo/, we don't care about /foo/bar/
                if ( delimiter != null && resultPage.getNextMarker().endsWith( delimiter ) )
                {
                    if (resultPage.getNextMarker().equals(prefix))
                    {
                        batchClauses.add( Require.not( Require.beanPropertyEquals( S3Object.NAME, prefix ) ) );
                    }
                    else //unless it's our original prefix, make sure we don't look for other things in this "directory"
                    {
                        batchClauses.add( Require.not(
                                Require.beanPropertyMatches(
                                        S3Object.NAME,
                                        Sanitize.patternLiteral( new StringBuilder(), resultPage.getNextMarker() ).append( '%' ).toString() ) )
                        );
                    }
                }
            }
            objects = objectService.retrieveAll( Query
                    .where( Require.all( batchClauses ) )
                    .orderBy( ordering )
                    .limit( maxKeys ) ).toList();
            if  (objects.isEmpty() )
            {
                resultPage.setNextMarker( null );
                return resultPage;
            }
        } while ( addObjectsToResultPage( resultPage, objects, prefix, delimiter, maxKeys ) );
        return resultPage;
    }


    static final class ResultPage
    {
        public String getNextMarker()
        {
            return m_nextMarker;
        }

        public void setNextMarker( final String nextMarker )
        {
            m_nextMarker = nextMarker;
        }

        public List< S3Object > getObjects()
        {
            return m_objects;
        }

        public Set< String > getPrefixes()
        {
            return m_prefixes;
        }

        public int numKeys()
        {
            return m_objects.size() + m_prefixes.size();
        }
        
        private final List< S3Object > m_objects = new ArrayList<>();
        private final Set< String > m_prefixes = new TreeSet<>();
        private String m_nextMarker = null;
    }//end inner class
    

    private static boolean addObjectsToResultPage(
    		final ResultPage resultPage,
			final List<S3Object> objects,
            final String prefix,
            final String delimiter,
            final int maxKeys ) {   
            
        for ( final S3Object object : objects )
        {
            if ( resultPage.numKeys() >= maxKeys )
            {
                return false; //we didn't consume all objects we were given
            }
            final String extractedPrefix = extractPrefix( prefix, delimiter, object.getName() );
            if ( extractedPrefix != null )
            {
                resultPage.getPrefixes().add( extractedPrefix );
                resultPage.setNextMarker( extractedPrefix );
            }
            else
            {
                resultPage.getObjects().add( object );
                resultPage.setNextMarker( object.getName() );
            }
        }
        return true; //we consumed all objects we were given
    }


    static String extractPrefix( final String prefix, final String delimiter, final String target )
    {
        if ( delimiter == null )
        {
            return null;
        }
        final int resultLength = target.indexOf( delimiter, ( prefix == null ) ? 0 : prefix.length() );
        if ( resultLength < 0 )
        {
            return null;
        }
        return target.substring( 0, resultLength + 1 );
    }
}
