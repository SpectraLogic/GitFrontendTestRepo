package com.spectralogic.s3.dataplanner.backend.tape.task;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.tape.domain.*;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Platform;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public abstract class BaseBlobTask extends BaseTapeTask {

    protected BaseBlobTask(final BlobStoreTaskPriority priority,
                           final UUID tapeId,
                           final DiskManager diskManager,
                           final TapeFailureManagement tapeFailureManagement,
                           final BeansServiceManager serviceManager) {
        super(priority, tapeId, tapeFailureManagement, serviceManager);
        m_diskManager = diskManager;
    }

    // This constructor is used for testing when a larger max retries is needed in order for tests to run in a
    // timely fashion.
    protected BaseBlobTask(final BlobStoreTaskPriority priority,
                           final UUID tapeId,
                           final DiskManager diskManager,
                           final TapeFailureManagement tapeFailureManagement,
                           final BeansServiceManager serviceManager,
                           final int maxRetriesBeforeSuspensionRequired) {
        super(priority, tapeId, tapeFailureManagement, serviceManager, maxRetriesBeforeSuspensionRequired);
        m_diskManager = diskManager;
    }


    final protected long getTotalWorkInBytes( final S3ObjectsOnMedia request )
    {
        long retval = 0;
        for ( final BucketOnMedia bucket : request.getBuckets() )
        {
            for ( final S3ObjectOnMedia o : bucket.getObjects() )
            {
                for ( final BlobOnMedia b : o.getBlobs() )
                {
                    retval += b.getLength();
                }
            }
        }

        return retval;
    }

    //used by read and verify jobs - we must retrieve the LocalJobEntryWork to get the order index
    final protected S3ObjectsIoRequest constructObjectsIoRequestFromJobEntries(
            final JobRequestType requestType,
            Set<JobEntry> jobEntries) {
        final Set< UUID > entryIds = BeanUtils.extractPropertyValues(jobEntries, JobEntry.ID );
        final List<LocalJobEntryWork> sortedEntries =
                getServiceManager().getRetriever(LocalJobEntryWork.class).retrieveAll(
                        Query.where(Require.beanPropertyEqualsOneOf(Identifiable.ID, entryIds))
                                .orderBy(new BeanSQLOrdering())).toList();
        return constructOrderedObjectsIoRequest(requestType, sortedEntries);
    }

    //used directly by verify tape task - there are no associated JobEntry records to get the order from
    final protected <T extends BlobObservable & OrderedEntry >S3ObjectsIoRequest constructOrderedObjectsIoRequest(
            final JobRequestType requestType,
            List<T> jobEntries) {
        Collections.sort(jobEntries, new BeanComparator<>(OrderedEntry.class, OrderedEntry.ORDER_INDEX));
        return constructObjectsIoRequest(requestType, jobEntries);
    }

    //used by writes directly, no sort by order index needed
    final protected S3ObjectsIoRequest constructObjectsIoRequest(
            final JobRequestType requestType,
            List<? extends BlobObservable> jobEntries)
    {

        final Set<UUID> blobIds = new HashSet<>(
                BeanUtils.< UUID >extractPropertyValues(jobEntries, BlobObservable.BLOB_ID ) );
        final Map< UUID, Blob> blobs = BeanUtils.toMap(
                getServiceManager().getRetriever( Blob.class ).retrieveAll( blobIds ).toSet() );
        final Set< UUID > objectIds = new HashSet<>(
                BeanUtils.< UUID >extractPropertyValues( blobs.values(), Blob.OBJECT_ID ) );
        final Map< UUID, S3Object> objects = BeanUtils.toMap(
                getServiceManager().getRetriever( S3Object.class ).retrieveAll( objectIds ).toSet() );

        final List< S3ObjectIoRequest > requests = new ArrayList<>();
        final List< UUID > objectIdOrder = new ArrayList<>();
        final Map< UUID, List< Blob > > blobsPerObject = new HashMap<>();
        final Set<UUID> blobIdsAlreadyInRequest = new HashSet<>();
        for ( final BlobObservable e : jobEntries)
        {
            if (blobIdsAlreadyInRequest.contains(e.getBlobId())) {
                continue;
            }
            final Blob blob = blobs.get( e.getBlobId() );
            if ( !blobsPerObject.containsKey( blob.getObjectId() ) )
            {
                objectIdOrder.add( blob.getObjectId() );
                blobsPerObject.put( blob.getObjectId(), new ArrayList<>() );
            }
            blobsPerObject.get( blob.getObjectId() ).add( blob );
            blobIdsAlreadyInRequest.add(e.getBlobId());
        }
        for ( final UUID objectId : objectIdOrder )
        {
            requests.add( constructObjectIoRequest(
                    requestType, objects.get( objectId ), blobsPerObject.get( objectId ) ) );
        }

        final S3ObjectsIoRequest retval = BeanFactory.newBean( S3ObjectsIoRequest.class );
        retval.setBuckets( CollectionFactory.toArray(
                BucketIoRequest.class, constructBucketIoRequests( objects, requests ) ) );
        if ( objects.isEmpty() )
        {
            throw new IllegalStateException(
                    "There are no objects or blobs to construct an I/O request for." );
        }

        if ( JobRequestType.VERIFY == requestType )
        {
            retval.setCacheRootPath( Platform.FILE_SEPARATOR + "dev" + Platform.FILE_SEPARATOR + "null" );
        }
        else
        {
            final Set< CacheFilesystem > cacheFilesystems = getServiceManager().getRetriever( CacheFilesystem.class ).retrieveAll().toSet();
            if (cacheFilesystems.size() > 1) {
                throw new IllegalStateException(
                        "Multiple cache filesystems found when only one was expected.");
            } else if (cacheFilesystems.isEmpty()) {
                throw new IllegalStateException(
                        "No cache filesystem found when one was expected.");
            }
            retval.setCacheRootPath( cacheFilesystems.iterator().next().getPath() );
        }
        return retval;
    }


    final protected List< BucketIoRequest > constructBucketIoRequests(
            final Map< UUID, S3Object > objects,
            final List< S3ObjectIoRequest > objectIoRequests )
    {
        final Map< UUID, List< S3ObjectIoRequest > > requests = new HashMap<>();
        for ( final S3ObjectIoRequest request : objectIoRequests )
        {
            final UUID bucketId = objects.get( request.getId() ).getBucketId();
            if ( !requests.containsKey( bucketId ) )
            {
                requests.put( bucketId, new ArrayList<>() );
            }
            requests.get( bucketId ).add( request );
        }

        final Set< Bucket > buckets = BeanUtils.sort(
                getServiceManager().getRetriever( Bucket.class ).retrieveAll( requests.keySet() ).toSet() );
        final List< BucketIoRequest > retval = new ArrayList<>();
        for ( final Bucket bucket : buckets )
        {
            final BucketIoRequest bucketIoRequest = BeanFactory.newBean( BucketIoRequest.class );
            bucketIoRequest.setBucketName( bucket.getName() );
            bucketIoRequest.setObjects(
                    CollectionFactory.toArray( S3ObjectIoRequest.class, requests.get( bucket.getId() ) ) );
            retval.add( bucketIoRequest );
        }

        return retval;
    }


    final protected S3ObjectIoRequest constructObjectIoRequest(
            final JobRequestType requestType,
            final S3Object o,
            final List< Blob > blobs )
    {
        if (JobRequestType.PUT == requestType) {
            Collections.sort(blobs, new BeanComparator<>(Blob.class, Blob.BYTE_OFFSET));
        }

        final S3ObjectIoRequest retval = BeanFactory.newBean( S3ObjectIoRequest.class );
        retval.setObjectName( o.getName() );
        if ( JobRequestType.PUT == requestType || JobRequestType.VERIFY == requestType )
        {
            final List< S3ObjectMetadataKeyValue > metadata = new ArrayList<>();
            for ( final S3ObjectProperty property
                    : getServiceManager().getRetriever( S3ObjectProperty.class ).retrieveAll(
                    Require.beanPropertyEquals( S3ObjectProperty.OBJECT_ID, o.getId() ) ).toSet() )
            {
                metadata.add( BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                        .setKey( property.getKey() ).setValue( property.getValue() ) );
            }
            if ( null != o.getCreationDate() )
            {
                metadata.add( BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                        .setKey( KeyValueObservable.CREATION_DATE )
                        .setValue( String.valueOf( o.getCreationDate().getTime() ) ) );
                if ( JobRequestType.VERIFY == requestType )
                {
                    metadata.add( getTotalBlobCountMetadata( o ) );
                }
            }
            if ( JobRequestType.PUT == requestType )
            {
                metadata.add( getTotalBlobCountMetadata( o ) );
            }
            retval.setMetadata( CollectionFactory.toArray( S3ObjectMetadataKeyValue.class, metadata ) );
        }

        final List< BlobIoRequest > requests = new ArrayList<>();
        for ( final Blob blob : blobs )
        {
            if ( o.getId().equals( blob.getObjectId() ) )
            {
                requests.add( constructBlobIoRequest( requestType, blob ) );
            }
        }
        retval.setBlobs( CollectionFactory.toArray( BlobIoRequest.class, requests ) );
        retval.setId( o.getId() );

        return retval;
    }


    private S3ObjectMetadataKeyValue getTotalBlobCountMetadata( final S3Object o )
    {
        return BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                .setKey( KeyValueObservable.TOTAL_BLOB_COUNT )
                .setValue( String.valueOf( getServiceManager().getRetriever( Blob.class ).getCount(
                        Blob.OBJECT_ID, o.getId() ) ) );
    }


    final protected BlobIoRequest constructBlobIoRequest( final JobRequestType requestType, final Blob blob )
    {
        final BlobIoRequest retval = BeanFactory.newBean( BlobIoRequest.class );
        final String fullPath = getBlobPath( requestType, blob.getId() );
        Path path = Paths.get( fullPath );
        int nameCount = path.getNameCount();
        /*
         * The else clause is leakage from the unit tests.
         * It would be preferable to set the cache directory to '/' to deal with pool requests, but that affects some
         * code that was intended to work with clustering - we don't currently expect to support clustering, but
         * leaving this as is to be dealt with as part of a larger refactor to remove "Nodes" and other clustering
         * artifacts - Kyle Hughart 11/17/17
         */
        if ( fullPath.startsWith("/pool") )
        {
            retval.setFileName( "../../../../.." + fullPath );
        }
        else if ( nameCount >= 3 )
        {
            retval.setFileName( path.subpath( nameCount - 3, nameCount )
                    .toString() );
        }
        else
        {
            final int lastFileSepIndex = fullPath.lastIndexOf( Platform.FILE_SEPARATOR );
            retval.setFileName( fullPath.substring( lastFileSepIndex + 1 ) );
        }
        retval.setOffset( blob.getByteOffset() );
        retval.setLength( blob.getLength() );
        retval.setChecksum( blob.getChecksum() );
        retval.setChecksumType( blob.getChecksumType() );
        retval.setId( blob.getId() );
        return retval;
    }


    private String getBlobPath( final JobRequestType requestType, final UUID blobId )
    {
        switch ( requestType )
        {
            case PUT:
                return m_diskManager.getDiskFileFor( blobId ).getFilePath();
            case GET:
                return m_diskManager.allocateChunksForBlob( blobId );
            case VERIFY:
                return Platform.FILE_SEPARATOR + "dev" + Platform.FILE_SEPARATOR + "null";
            default:
                throw new UnsupportedOperationException( "No code for " + requestType + "." );
        }
    }

    protected final DiskManager m_diskManager;
}
