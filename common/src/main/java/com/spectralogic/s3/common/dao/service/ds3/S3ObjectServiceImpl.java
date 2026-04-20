/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.notification.BucketChangesNotificationRegistration;
import com.spectralogic.s3.common.dao.domain.notification.BucketHistoryEvent;
import com.spectralogic.s3.common.dao.domain.notification.BucketHistoryEventType;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.orm.BucketHistoryEventRM;
import com.spectralogic.s3.common.dao.orm.BucketRM;
import com.spectralogic.s3.common.dao.orm.S3ObjectRM;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.notification.domain.event.BucketNotificationEvent;
import com.spectralogic.s3.common.platform.notification.generator.BucketChangesNotificationPayloadGenerator;
import com.spectralogic.s3.common.platform.persistencetarget.PersistenceTargetUtil;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectFailure;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectFailureReason;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectsResult;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.db.service.api.NestableTransaction;
import com.spectralogic.util.db.service.api.RetrieveBeansResult;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

final class S3ObjectServiceImpl extends BaseService< S3Object > 
    implements S3ObjectService
{
    S3ObjectServiceImpl()
    {
        super( S3Object.class,
               AWSFailure.NO_SUCH_OBJECT );
    }
    
    
    public int retrieveNumberOfObjectsInBucket( final UUID bucketId )
    {
        return getDataManager().getCount( S3Object.class,
                Require.beanPropertyEquals( S3Object.BUCKET_ID, bucketId ) );
    }
    
    
    public UUID retrieveId(
    		final String bucketName,
    		final String objectName,
    		final UUID versionId,
    		final boolean includeIncomplete )
    {
    	return retrieveId(bucketName, objectName, versionId, includeIncomplete, null );
    }
    
    
    public UUID retrieveId(
    		final String bucketName,
    		final String objectName,
    		final UUID versionId,
    		//TODO: the actual function here is more like "don't require latest if version ID is null"
    		//rather than "includeIncomplete" improve structure and/or naming.
    		final boolean includeIncomplete, 
    		final UUID jobId )
    {
        final Bucket bucket = 
                getServiceManager().getRetriever( Bucket.class ).retrieve( Bucket.NAME, bucketName );

        if ( null == bucket )
        {
            return null;
        }
        final DataPolicy dataPolicy =
                new BucketRM( bucket, getServiceManager() )
                        .getDataPolicy().unwrap();
        final WhereClause versionFilter;
        if ( null != versionId )
        {
            versionFilter = Require.beanPropertyEquals( Identifiable.ID, versionId );
        }
        else if ( !includeIncomplete )
        {
            versionFilter = Require.beanPropertyEquals( S3Object.LATEST, Boolean.TRUE );
        }
        else
        {
        	versionFilter = Require.nothing();
        }

        final WhereClause jobFilter;
        if ( null == jobId )
        {
        	jobFilter = Require.nothing();
        }
        else
        {
        	jobFilter = Require.exists(
		        				Blob.class,
		        				Blob.OBJECT_ID,
		        				Require.exists(
		        						JobEntry.class,
		        						BlobObservable.BLOB_ID,
		        						Require.exists(
		        								JobEntry.JOB_ID,
		        								Require.beanPropertyEquals( Identifiable.ID, jobId ) ) ) );
        }
        
        final Set< S3Object > results = getDataManager().getBeans( S3Object.class, Query.where( Require.all(
        		jobFilter,
                versionFilter,
                Require.beanPropertyEquals( S3Object.BUCKET_ID, bucket.getId() ),
                Require.beanPropertyEquals( S3Object.NAME, objectName ) ) ) ).toSet();
        if ( 0 == results.size() )
        {
            return null;
        }
        else if ( 1 < results.size() )
        {
            if (dataPolicy.getVersioning().equals(VersioningLevel.KEEP_MULTIPLE_VERSIONS)) {
                Optional<S3Object> latestObjectVersion = results.stream()
                        .filter(S3Object::isLatest).findFirst();
                if (latestObjectVersion.isPresent()) {
                    return latestObjectVersion.get().getId();
                } else {
                    LOG.warn("Latest flag not set for objects with data policy using versioning. Multiple possible matches were found for object. Matches count: " + results.size() + ". ");
                    throw new DaoException( GenericFailure.BAD_REQUEST, "Multiple possible matches were found.");
                }
            }
            if (!dataPolicy.getVersioning().equals(VersioningLevel.KEEP_LATEST) ) {
                LOG.warn("Multiple possible matches were found for object. Matches count: " + results.size() + ". ");
                throw new DaoException( GenericFailure.BAD_REQUEST, "Multiple possible matches were found.");
            }
        }
        return results.iterator().next().getId();
    }


    
    public UUID attainId(
    		final String bucketName,
    		final String objectName,
    		final UUID versionId,
    		final boolean includeIncomplete )
    {
        final UUID retval = retrieveId( bucketName, objectName, versionId, includeIncomplete );
        if ( null == retval )
        {
            throw new DaoException( AWSFailure.NO_SUCH_OBJECT, "Object does not exist." );
        }
        return retval;
    }
    
    
    public long getSizeInBytes( final Set< UUID > objectIds )
    {
        return getDataManager().getSum(
                Blob.class,
                Blob.LENGTH,
                Require.beanPropertyEqualsOneOf( Blob.OBJECT_ID, objectIds ) );
    }
    
    
    public long getSizeInBytes( final UUID objectId )
    {
        return getSizeInBytes( CollectionFactory.toSet( objectId ) );
    }
    
    
    public boolean isEveryBlobReceived( final UUID objectId )
    {
        return ( 0 == getDataManager().getCount( Blob.class, Require.all(
                Require.beanPropertyEquals( Blob.OBJECT_ID, objectId ),
                Require.beanPropertyEquals( ChecksumObservable.CHECKSUM, null ) ) ) );
    }
    
    
    @Override
    public void create( final Set< S3Object > objects )
    {
        verifyInsideTransaction();
        if ( objects.isEmpty() )
        {
            return;
        }
        
        for ( final S3Object o : objects )
        {
        	o.setLatest( false );
        	o.setType( S3ObjectType.fromObjectName( o.getName() ) );
        }

        validate( objects );
        checkAgainstExistingObjects( objects );
        S3ObjectServiceImpl.super.create( objects );
    }
    
    
    public Set< UUID > getObjectsForBlobs( final Set< UUID > blobIds )
    {
        return BeanUtils.toMap( getServiceManager().getRetriever( S3Object.class ).retrieveAll(
                Require.exists(
                        Blob.class,
                        Blob.OBJECT_ID, 
                        Require.beanPropertyEqualsOneOf( 
                                Identifiable.ID, blobIds ) ) ).toSet() ).keySet();
    }
    
    
    public void markObjectsReceived( final Set< S3Object > objects )
    {
        final List<BucketHistoryEvent> changes = new ArrayList<>();
        try ( final NestableTransaction transaction = getServiceManager().startNestableTransaction() )
        {
            for ( S3Object obj : objects )
            {
                if ( null == obj.getCreationDate() )
                {
                    obj.setCreationDate( new Date() );
                }
                    changes.add( createChange( obj, BucketHistoryEventType.CREATE ) );
                    final S3Object duplicateObject = getCurrentLatestObject( obj );
                    if ( null == duplicateObject )
                    {
                        obj.setLatest( true );
                        changes.add( createChange( obj, BucketHistoryEventType.MARK_LATEST ) );

                    }
                    else
                    {
                        final Set< S3Object > duplicateObjects = new HashSet<>();
                        duplicateObjects.add( obj );
                        duplicateObjects.add( duplicateObject );
                        //NOTE: we sort here instead of simply checking the creation date because if the creation date
                        //is a tie, we want to determine via UUID in a consistent way.
                        final Set< S3Object > sortedSet = BeanUtils.sort( duplicateObjects );
                        if ( sortedSet.iterator().next().getId().equals( obj.getId() ) )
                        {
                            update( duplicateObject.setLatest( false ), S3Object.LATEST );
                            changes.add( createChange(duplicateObject, BucketHistoryEventType.UNMARK_LATEST ) );
                            obj.setLatest( true );
                            changes.add( createChange( obj, BucketHistoryEventType.MARK_LATEST ) );
                        }
                    }
                    update( obj, S3Object.LATEST, S3Object.CREATION_DATE );
            }
            for (BucketHistoryEvent change : changes) {
                transaction.getCreator(BucketHistoryEvent.class).create(change);
            }
            transaction.commitNestableTransaction();
            queueBucketChangesEvent( changes );
        }
    }


    private void queueBucketChangesEvent( final List< BucketHistoryEvent > changes ) {
        if ( !changes.isEmpty() ) {
            long minSequenceNumber = new BucketHistoryEventRM( changes.get(0).getId(), getServiceManager() ).getSequenceNumber();
            getServiceManager().getNotificationEventDispatcher().queueFire( new BucketNotificationEvent(
                    new BucketRM(changes.get(0).getBucketId(), getServiceManager()).unwrap(),
                    getServiceManager().getRetriever( BucketChangesNotificationRegistration.class ),
                    new BucketChangesNotificationPayloadGenerator( minSequenceNumber, getServiceManager().getRetriever(BucketHistoryEvent.class ) ) ) );
        }
    }


    public S3Object getMostRecentVersion( final UUID bucketId, final String objectName )
    {
    	final WhereClause duplicateObjectFilter = Require.all( 
                Require.beanPropertyEquals( 
                        S3Object.BUCKET_ID,
                        bucketId ),
                Require.beanPropertyEquals( 
                        S3Object.NAME,
                        objectName ) );
		final Set< S3Object > duplicateObjects = retrieveAll( duplicateObjectFilter ).toSet();
    	return BeanUtils.sort( duplicateObjects ).iterator().next();
    }
    
    
    public void markObjectReceived( final S3Object object, final Date creationDate )
    {
    	object.setCreationDate( creationDate );
    	markObjectsReceived( CollectionFactory.toSet( object ) );
    }
    
    
    public void markObjectReceived( final S3Object object )
    {
    	markObjectsReceived( CollectionFactory.toSet( object ) );
    }
    
    
    private void checkAgainstExistingObjects( final Set< S3Object > objects )
    {
    	final DataPolicy dataPolicy = new S3ObjectRM( objects.iterator().next(),
                getServiceManager() ).getBucket().getDataPolicy().unwrap();
    	final Set< String > objectNames = new HashSet<>(); 
        if (VersioningLevel.NONE == dataPolicy.getVersioning() )
        {
	        final Set< S3Object > duplicateObjects = new HashSet<>();
	        for ( S3Object obj : objects )
	        {
	        	if ( objectNames.contains( obj.getName() ) )
	        	{
	        		throw new DaoException( GenericFailure.BAD_REQUEST, " Object \"" + obj.getName()
	        				+ "\" appears twice in the same request." );
	        	}
	        	objectNames.add( obj.getName() );
	        	if ( hasDuplicateObject( obj ) )
	        	{
	        		duplicateObjects.add( obj );
	        	}
	        }
	        if ( !duplicateObjects.isEmpty() )
	        {
                final List< String > duplicateObjectNames = new ArrayList<>( 
                        BeanUtils.extractPropertyValues( duplicateObjects, S3Object.NAME ) );
                Collections.sort( duplicateObjectNames );
                throw new DaoException(
                        AWSFailure.OBJECT_ALREADY_EXISTS,
                        "Conflicts exist with existing data objects: " + duplicateObjectNames );  
	        }
        }
    }
    
    
    private boolean hasDuplicateObject( final S3Object obj )
    {
    	final WhereClause duplicateObjectFilter = Require.all( 
                Require.beanPropertyEquals( 
                        S3Object.BUCKET_ID,
                        obj.getBucketId() ),
                Require.beanPropertyEquals( 
                        S3Object.NAME,
                        obj.getName() ) );
        return any( duplicateObjectFilter );
    }
    
    
    private S3Object getCurrentLatestObject( final S3Object obj )
    {
    	final WhereClause duplicateObjectFilter = Require.all( 
                Require.beanPropertyEquals( 
                        S3Object.BUCKET_ID,
                        obj.getBucketId() ),
                Require.beanPropertyEquals( 
                        S3Object.NAME,
                        obj.getName() ),
                Require.beanPropertyEquals( 
                        S3Object.LATEST,
                        true ) );
        return retrieveAll( duplicateObjectFilter ).getFirst();
    }
    
    
    private void validate( final Set< S3Object > objects )
    {
        final UUID bucketId = objects.iterator().next().getBucketId();
        final Duration duration = new Duration();
        VersioningLevel versioning = null;
        if ( !objects.isEmpty() )
        {
        	versioning = new S3ObjectRM(objects.iterator().next(), getServiceManager()).getBucket().getDataPolicy().getVersioning();
        }       
        
        final Set< String > allNames = new HashSet<>();
        for ( final S3Object object : objects )
        {
            if ( !bucketId.equals( object.getBucketId() ) )
            {
                throw new DaoException( 
                        GenericFailure.BAD_REQUEST,
                        "When bulk creating objects, they must belong to the same bucket." );
            }
            if ( VersioningLevel.NONE == versioning && !allNames.add( object.getName() ) )
            {
                throw new DaoException( 
                        GenericFailure.BAD_REQUEST,
                        "Object or folder was declared twice in create request: " + object.getName() );
            }
        }
    
        LOG.info( "Validated request to create " + objects.size() + " objects in "
                + duration + "." );
    }
    
    
    @Override
    public void delete( final UUID objectId )
    {
        getDataManager().deleteBeans( Blob.class, Require.beanPropertyEquals( Blob.OBJECT_ID, objectId ) );
        super.delete( objectId );
    }


    @Override
    public void deleteByBucketId( final UUID bucketId )
    {
        getDataManager().deleteBeans( Blob.class, Require.exists(
                Blob.OBJECT_ID,
                Require.beanPropertyEquals( S3Object.BUCKET_ID, bucketId ) ) );
        getDataManager().deleteBeans(
                S3Object.class,
                Require.beanPropertyEquals( S3Object.BUCKET_ID, bucketId ) );
    }

    
    public ReentrantReadWriteLock getLock()
    {
        if ( getServiceManager().isTransaction() )
        {
            return getServiceManager().getTransactionSource().getService( S3ObjectService.class ).getLock();
        }
        return m_lock;
    }
    
    
    public void deleteLegacyObjectsIfEntirelyPersisted( final Set< UUID > objectIds )
    {
        if ( objectIds.isEmpty() )
        {
            return;
        }
        for ( final UUID objectId : objectIds )
        {
        	if ( null == retrieve(objectId) )
        	{
        		LOG.info("Will not check if object " + objectId + " is fully persisted because it no longer exists.");
        		continue;
        	}
        	final S3ObjectRM object = new S3ObjectRM( objectId, getServiceManager() );
        	final VersioningLevel versioningLevel = object.getBucket().getDataPolicy().getVersioning();
        	int versionsToKeep;
        	if ( versioningLevel.equals( VersioningLevel.KEEP_LATEST ) )
        	{
        		versionsToKeep = 1;
        	}
        	else if ( versioningLevel.equals( VersioningLevel.KEEP_MULTIPLE_VERSIONS ) )
        	{
        		versionsToKeep = object.getBucket().getDataPolicy().unwrap().getMaxVersionsToKeep();
        	}
        	else
        	{
        		//NOTE: If versioning == NONE, we should have already conflicted in checkAgainstExistingObjects
        		continue;
        	}
            //NOTE: The only way our object might not be fully persisted following the completion of the current chunk
            //is if it is a multi-blob object
            final boolean singleBlobObject = object.getBlobs().toSet().size() == 1;
            if ( singleBlobObject || PersistenceTargetUtil.isObjectFullyPersisted( objectId , getServiceManager() ) )
            {
                //We do a quick preliminary version count to avoid an expensive query for objects that don't have any
                //duplicates (which is presumably the most typical case).
                final int versionCount = getServiceManager().getRetriever(S3Object.class).getCount(Require.all(
                        Require.beanPropertyEquals(S3Object.NAME, object.getName()),
                        Require.beanPropertyEquals(S3Object.BUCKET_ID, object.unwrap().getBucketId())
                ));
                if (versionCount > versionsToKeep) {
                    LOG.info( "Object " + object.getId() + " (" + object.getName() + ") has been entirely persisted. Will age out old versions as needed." );
                    //NOTE: An object that is eligible to be deleted / aged out may delete itself in this situation.
                    //We do not consider objects that are still part of a job, even if they are older, in order to avoid
                    //truncating jobs. The object will eventually finish persisting, hit this code, and delete itself
                    //if appropriate. We avoid truncating jobs from here because we don't want concurrent replicated
                    //put jobs to risk truncating each other and confusing the source BP.
                    final List< S3Object > duplicates =
                            getSortedDuplicateObjects( object.unwrap(), false, true, true).toList();
                    for ( final S3Object v : duplicates )
                    {
                        if ( versionsToKeep-- <= 0 )
                        {
                            LOG.info( "Version " + v.getId() + " can be deleted." );
                            delete( PreviousVersions.DELETE_SPECIFIC_VERSION, Collections.singleton( v.getId() ) );
                        }
                    }
                } else {
                    LOG.info( "Object " + object.getId() + " (" + object.getName() + ") has been entirely persisted." );
                }
            }
        }
    }
    
    
    public DeleteResult delete( final PreviousVersions previousVersions, final Set< UUID > objectIds )
    {
        return delete( previousVersions, objectIds, null );
    }
    
    
    public DeleteResult delete( 
            final PreviousVersions previousVersions,
            final Set< UUID > objectIds,
            final DeleteS3ObjectsPreCommitListener listener )
    {
        WhereClause filter = Require.beanPropertyEqualsOneOf(Identifiable.ID, objectIds);
        if (previousVersions == PreviousVersions.UNMARK_LATEST) {
            filter = Require.all(filter, Require.beanPropertyEquals(S3Object.LATEST, true));
        }
        final List< S3Object > objects = retrieveAll( filter ).toList();
        if (objects.isEmpty()) {
            if ( null != listener )
            {
                //we signal the listener if there is one there is nothing to commit.
                listener.preparedToCommitDelete( getServiceManager() );
            }
            return new DeleteResultImpl( null, objectIds, Collections.emptyList(), Collections.emptySet() );
        }

        BucketRM bucket = new S3ObjectRM(objects.get( 0 ), getServiceManager()).getBucket();

        final BucketLogicalSizeCache bucketLogicalSizeCache =
                getServiceManager().getService( BucketService.class ).getLogicalSizeCache();
        final Set< UUID > deletedBlobs = new HashSet<>();
        final List<BucketHistoryEvent> changes = new ArrayList<>();
        try ( final NestableTransaction transaction = getServiceManager().startNestableTransaction() )
        {
            final S3ObjectServiceImpl objectService =
                    (S3ObjectServiceImpl)transaction.getService( S3ObjectService.class );
    
            for ( final S3Object object : new HashSet<>( objects ) )
            {
                switch ( previousVersions )
                {
                    case UNMARK_LATEST:
                        changes.add( createChange( object, BucketHistoryEventType.UNMARK_LATEST ) );
                		objectService.update( object.setLatest( false ), S3Object.LATEST );
                		break;
                    case DELETE_SPECIFIC_VERSION:
                    	if ( object.isLatest() )
                    	{
                    		S3Object objectToRollbackTo =
                    				getSortedDuplicateObjects( object, true, true, false ).getFirst();
                    		if ( null != objectToRollbackTo )
                    		{
		                    	objectService.update( objectToRollbackTo.setLatest( true ), S3Object.LATEST );
                                changes.add( createChange( objectToRollbackTo, BucketHistoryEventType.MARK_LATEST ) );
			                    LOG.info( "Rolled back object " + object.getBucketId() + "." + object.getName()
			                              + " from version " + object.getId() + " to " + objectToRollbackTo.getId() 
			                              + "." );
                    		}
                    	}
                        break;
                    case DELETE_ALL_VERSIONS:
                        objects.addAll( getSortedDuplicateObjects( object, false, false, false ).toSet() );
                        break;
                    default:
                        throw new UnsupportedOperationException( "No code for: " + previousVersions );
                }
            }
            
            if ( PreviousVersions.UNMARK_LATEST == previousVersions )
            {
            	if ( !isTransaction() )
                {
                    for ( BucketHistoryEvent change : changes )
                    {
                        transaction.getCreator( BucketHistoryEvent.class ).create( change );
                    }
                    transaction.commitTransaction();
                }
                queueBucketChangesEvent( changes );
            	return new DeleteResultImpl( bucket.getName(), objectIds, objects, new HashSet<>() );
            }
    
            final BlobService blobService = transaction.getService( BlobService.class );
            long blobsDeletedSize = 0;
            final Duration durationSinceLastLog = new Duration();
            final Duration totalDuration = new Duration();
            int currentObjectCount = 0;
            final int totalFolderCount = objects.size();
    
            for ( final S3Object object : objects )
            {
                ++currentObjectCount;
                if ( 0 < durationSinceLastLog.getElapsedMinutes() )
                {
                    LOG.info( String.format( "Deleting object %d of %d from database, ~%.0fs left", currentObjectCount,
                            totalFolderCount, ( totalDuration.getElapsedSeconds() / ( double ) currentObjectCount ) *
                                    ( totalFolderCount - currentObjectCount ) ) );
                    durationSinceLastLog.reset();
                }
    
                final WhereClause whereClauseCompletedBlobs =
                        Require.all( Require.not( Require.beanPropertyEquals( ChecksumObservable.CHECKSUM, null ) ),
                                Require.beanPropertyEquals( Blob.OBJECT_ID, object.getId() ) );
                blobsDeletedSize += blobService.getSum( Blob.LENGTH, whereClauseCompletedBlobs );
    
                final WhereClause whereClauseAllBlobs =
                        Require.all( Require.beanPropertyEquals( Blob.OBJECT_ID, object.getId() ) );
                deletedBlobs.addAll( blobService.retrieveAll( whereClauseAllBlobs )
                                                .toSet()
                                                .stream()
                                                .map( Identifiable::getId )
                                                .collect( Collectors.toSet() ) );
                changes.add( createChange( object, BucketHistoryEventType.DELETE ) );
                objectService.delete( object.getId() );
            }
    
            if ( null != listener )
            {
                listener.preparedToCommitDelete( transaction );
            }
            for ( BucketHistoryEvent change : changes ) {
                transaction.getCreator( BucketHistoryEvent.class ).create( change );
            }
            transaction.commitNestableTransaction();
            queueBucketChangesEvent( changes );
            bucketLogicalSizeCache.blobDeleted( bucket.getId(), blobsDeletedSize );
        }
    
        return new DeleteResultImpl( bucket.getName(), objectIds, objects, deletedBlobs );
    }
    
    
    private RetrieveBeansResult<S3Object> getSortedDuplicateObjects(
    		final S3Object object,
    		final boolean excludeSelf,
    		final boolean fullyReceivedOnly,
    		final boolean excludeObjectsCurrentlyInPutJob )
    {
    	WhereClause objectFilter = Require.nothing();
    	if ( excludeObjectsCurrentlyInPutJob )
    	{
    		objectFilter = Require.not( Require.exists(
								Blob.class,
								Blob.OBJECT_ID,
			    				Require.exists(
						                JobEntry.class,
						                BlobObservable.BLOB_ID,
						                Require.exists( 
						                        JobEntry.JOB_ID,
						                        Require.beanPropertyEquals( 
						                                JobObservable.REQUEST_TYPE,
						                                JobRequestType.PUT ) ) ) ) );
    	}
    	if ( fullyReceivedOnly )
    	{
    		objectFilter = Require.all( objectFilter,
					Require.not( Require.beanPropertyEquals( S3Object.CREATION_DATE, null ) ) );
    	}
    	if ( excludeSelf )
    	{
    		objectFilter = Require.all( objectFilter,
					Require.not( Require.beanPropertyEquals( Identifiable.ID, object.getId() ) ) );
    	}
    	final BeanSQLOrdering ordering = new BeanSQLOrdering();
    	ordering.add( S3Object.CREATION_DATE, SortBy.Direction.DESCENDING );
        return retrieveAll( Query.where( Require.all(
        		Require.beanPropertyEquals( S3Object.BUCKET_ID, object.getBucketId() ),
        		Require.beanPropertyEquals( S3Object.NAME, object.getName() ),
        		objectFilter ) ).orderBy( ordering ) );
    }
    
    
    private final static class DeleteResultImpl implements DeleteResult
    {
        private DeleteResultImpl( final String bucketName, final Set< UUID > objectIds, final List< S3Object > objects,
                final Set< UUID > blobIds )
        {
            m_bucketName = bucketName;
            m_objectIds = objectIds;
            m_objects = objects;
            m_blobIds = blobIds;
        }


        public DeleteObjectsResult toDeleteObjectsResult()
        {
            final List< DeleteObjectFailure > deleteObjectFailures = new ArrayList<>();
            final Set< UUID > retrievedObjectIds = BeanUtils.toMap( m_objects ).keySet();
            for ( final UUID objectId : m_objectIds )
            {
                if ( !retrievedObjectIds.contains( objectId ) )
                {
                    final DeleteObjectFailure failure = BeanFactory.newBean( DeleteObjectFailure.class );
                    failure.setObjectId( objectId );
                    failure.setReason( DeleteObjectFailureReason.NOT_FOUND );
                    deleteObjectFailures.add( failure );
                }
            }
            final DeleteObjectsResult result = BeanFactory.newBean( DeleteObjectsResult.class );
            result.setDaoModified( !m_objects.isEmpty() );
            result.setFailures( deleteObjectFailures.toArray( new DeleteObjectFailure[ 0 ] ) );
            return result;
        }
    
    
        public Set< UUID > getBlobIds()
        {
            return m_blobIds;
        }
    
    
        public String getBucketName()
        {
            return m_bucketName;
        }
    
    
        public Set< UUID > getObjectIds()
        {
            return m_objectIds;
        }
    
    
        private final Set< UUID > m_blobIds;
        private final String m_bucketName;
        private final Set< UUID > m_objectIds;
        private final List< S3Object > m_objects;
    } // end inner class def
    

    private BucketHistoryEvent createChange(final S3Object obj, final BucketHistoryEventType type) {
        final BucketHistoryEvent bean = BeanFactory.newBean( BucketHistoryEvent.class )
                .setObjectName( obj.getName() )
                .setObjectCreationDate( obj.getCreationDate() )
                .setBucketId( obj.getBucketId() )
                .setVersionId( obj.getId() )
                .setType(type);
        bean.setId(UUID.randomUUID());
        return bean;
    }

    private final ReentrantReadWriteLock m_lock = new ReentrantReadWriteLock();
}
