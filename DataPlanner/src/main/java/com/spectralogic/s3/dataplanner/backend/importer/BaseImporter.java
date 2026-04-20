/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.importer;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.ImportDirective;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.service.ds3.BlobService;
import com.spectralogic.s3.common.dao.service.ds3.BucketLogicalSizeCache;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.DegradedBlobService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectPropertyService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.dao.service.shared.BlobLossRecorder;
import com.spectralogic.s3.common.dao.service.shared.ImportDirectiveService;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.BucketOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectMetadataKeyValue;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansRetrieverManager;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.log.ThrottledLog;
import com.spectralogic.util.security.ChecksumType;


abstract class BaseImporter< 
    BP extends DatabasePersistable & BlobObservable< BP >,
    ID extends ImportDirective< ID > & DatabasePersistable,
    F,
    H extends ImportHandler< F > >
{
    protected BaseImporter(
            final String targetDescription,
            final Class< BP > blobPersistenceTargetType,
            final WhereClause blobPersistenceTargetFilter,
            final F importFailedCode,
            final F importIncompleteCode,
            final Class< ? extends ImportDirectiveService< ID > > importDirectiveServiceType,
            final UUID idOfEntityToImport,
            final H importHandler,
            final BeansServiceManager serviceManager,
            final ThrottledLog noBlobCountLog )
    {
        m_targetDescription = targetDescription;
        m_blobPersistenceTargetType = blobPersistenceTargetType;
        m_blobPersistenceTargetFilter = blobPersistenceTargetFilter;
        m_importIncompleteCode = importIncompleteCode;
        m_importFailedCode = importFailedCode;
        m_importDirectiveService = serviceManager.getService( importDirectiveServiceType );
        
        ID directive = null;
        try
        {
            directive = m_importDirectiveService.attainByEntityToImport( idOfEntityToImport );
        }
        catch ( final DaoException ex )
        {
            LOG.warn( "Failed to attain directive for import.", ex );
        }
        m_directive = directive;
        m_idOfEntityToImport = idOfEntityToImport;
        m_importHandler = importHandler;
        m_serviceManager = serviceManager;
        m_noBlobCountLog = noBlobCountLog;
        m_importHandler.setImporter( this );

        Validations.verifyNotNull(
                "Target description", m_targetDescription );
        Validations.verifyNotNull(
                "Blob persistence target type", m_blobPersistenceTargetType );
        Validations.verifyNotNull(
                "Blob persistence target filter", m_blobPersistenceTargetFilter );
        Validations.verifyNotNull(
                "Import failed code", m_importFailedCode );
        Validations.verifyNotNull(
                "Import handler", m_importHandler );
        Validations.verifyNotNull(
                "Service manager", m_serviceManager );
    }


    final public BlobStoreTaskState run()
    {
        try
        {
            if ( null == m_directive )
            {
                throw new RuntimeException( "No directive exists specifying how the import should occur." );
            }
            
            boolean dataExists = false;
            initializeLostBlobs();
            m_importHandler.openForRead();
            while ( true )
            {
                final S3ObjectsOnMedia objectsOnMedia = m_importHandler.read();
                if ( null == objectsOnMedia )
                {
                    if ( !dataExists )
                    {
                        throw new RuntimeException(
                                "No data exists on " + m_targetDescription + " to import." );
                    }
                    recordLostBlobs();
                    deleteImportDirective();
                    reportIfImportWasIncomplete();
                    final BlobStoreTaskState importResult = finalizeImport( m_serviceManager );
                    for ( UUID objectId : m_completedObjectIds )
                    {
                    	m_serviceManager.getService( S3ObjectService.class )
                    		.deleteLegacyObjectsIfEntirelyPersisted( CollectionFactory.toSet( objectId ) );
                    }
                    return importResult;
                }

                dataExists = true;
                final F verifyResult = verifyPriorToImport( objectsOnMedia );
                if ( null != verifyResult )
                {
                    return m_importHandler.failed( 
                            verifyResult, 
                            new RuntimeException( "Import failed due to data integrity issue." ) );
                }

                for ( final BucketOnMedia mediaBucket : objectsOnMedia.getBuckets() )
                {
                    new Importer( mediaBucket );
                }
            }
        }
        catch ( final RuntimeException ex )
        {
        	LOG.warn( "Import " + m_targetDescription + " failed.", ex );
        	performFailureCleanup( m_serviceManager );
            return m_importHandler.failed( m_importFailedCode, ex );
        }
        finally
        {
            m_importHandler.closeRead();
        }
    }
    
    
    final private void reportIfImportWasIncomplete()
    {
        if ( 0 < m_objectsWithoutCreationDate )
        {
            m_importHandler.warn( m_importIncompleteCode,
                    new RuntimeException( m_objectsWithoutCreationDate + " objects did not have a creation" +
                            " date during import. These objects may be incomplete, or have blobs stored" +
                            " elsewhere." ) );
        }
    }
    
    
    final public void deleteImportDirective()
    {
        m_importDirectiveService.deleteByEntityToImport( m_idOfEntityToImport );
    }
    
    
    protected abstract BlobStoreTaskState finalizeImport( final BeansRetrieverManager brm );
    
    
    /**
     * @return null to indicate success; non-null to indicate 
     */
    protected abstract F verifyPriorToImport( final S3ObjectsOnMedia objectsOnMedia );


    private void initializeLostBlobs()
    {
        m_lostBlobs.clear();
        try ( final EnhancedIterable< BP > bpIterable = 
                m_serviceManager.getRetriever( m_blobPersistenceTargetType ).retrieveAll( 
                        m_blobPersistenceTargetFilter ).toIterable() )
        {
            for ( final BP bp : bpIterable )
            {
                m_lostBlobs.put( bp.getBlobId(), bp );
            }
        }

        LOG.info( m_targetDescription + " has " + m_lostBlobs.size() 
                  + " blobs previously known to reside on it." );
    }


    private void recordLostBlobs()
    {
        if ( m_lostBlobs.isEmpty() )
        {
            return;
        }

        final BeansServiceManager transaction = m_serviceManager.startTransaction();
        try
        {
            @SuppressWarnings( "unchecked" )
            final BlobLossRecorder< BP > blobLossRecorder = 
            (BlobLossRecorder< BP >)transaction.getRetriever( m_blobPersistenceTargetType );
            blobLossRecorder.blobsSuspect(
                    "Blobs no longer present after re-import.",
                    new HashSet<>( m_lostBlobs.values() ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
    }


    private final class Importer
    {
        private Importer( final BucketOnMedia bucketOnMedia )
        {
            m_bucket = attainBucket( bucketOnMedia );
            final DataPolicy dataPolicy =
                    m_serviceManager.getRetriever( DataPolicy.class ).attain( m_bucket.getDataPolicyId() );
            m_requiredChecksumType = dataPolicy.getChecksumType();
            m_ruleId = importBucketBegun( m_bucket, m_serviceManager );
            
            int orderIndex = -1;
            final boolean persistenceTargetTypeRequiresOrderIndices =
                    BeanUtils.hasPropertyName( m_blobPersistenceTargetType, BlobTape.ORDER_INDEX );
            if ( persistenceTargetTypeRequiresOrderIndices )
            {
                orderIndex = (int)m_serviceManager.getRetriever( m_blobPersistenceTargetType ).getMax(
                        BlobTape.ORDER_INDEX,
                        m_blobPersistenceTargetFilter );
            }
            for ( final S3ObjectOnMedia mediaObject : bucketOnMedia.getObjects() )
            { 
                if ( persistenceTargetTypeRequiresOrderIndices )
                {
                    for ( final BlobOnMedia bom : mediaObject.getBlobs() )
                    {
                        m_orderIndexes.put( bom.getId(), Integer.valueOf( ++orderIndex ) );
                    }
                }
                if ( !m_objectsOnMedia.containsKey( mediaObject.getObjectName() ) )
                {
                    m_objectsOnMedia.put( 
                            mediaObject.getObjectName(), new HashMap< UUID, S3ObjectOnMedia >() );
                }
                final UUID objectId = mediaObject.getId();
                final Map< UUID, S3ObjectOnMedia > mediaObjectVersions =
                        m_objectsOnMedia.get( mediaObject.getObjectName() );
                
                if ( !mediaObjectVersions.containsKey( objectId ) )
                {
                    mediaObjectVersions.put( objectId, mediaObject );
                }
                else
                {
                    S3ObjectOnMedia existingMediaObject = mediaObjectVersions.get( objectId );
                    BlobOnMedia[] combinedBlobArray = 
                            ArrayUtils.addAll( existingMediaObject.getBlobs(), mediaObject.getBlobs() );
                    existingMediaObject.setBlobs( combinedBlobArray );
                    if ( !haveMatchingMetadata(existingMediaObject, mediaObject) )
                    {
                        throw new RuntimeException( "Conflicting metadata entries for object " +
                                mediaObject.getObjectName() + " were found during import." );
                    }
                }
            }
            m_existingObjects = m_serviceManager.getRetriever( S3Object.class ).retrieveAll( Require.all( 
                    Require.beanPropertyEquals( S3Object.BUCKET_ID, m_bucket.getId() ),
                    Require.beanPropertyEqualsOneOf( S3Object.NAME, m_objectsOnMedia.keySet() ) ) )
                    .toSet();
            for ( final S3Object existingObject : m_existingObjects )
            {
                if ( !m_existingObjectNames.containsKey( existingObject.getName() ) )
                {
                    m_existingObjectNames.put( existingObject.getName(), new HashSet< UUID >() );
                }
                m_existingObjectNames.get( existingObject.getName() ).add( existingObject.getId() );
            }
            m_existingCreationDates = BeanUtils.extractPropertyValues(
                    m_serviceManager.getRetriever( S3ObjectProperty.class ).retrieveAll( Require.all( 
                            Require.beanPropertyEqualsOneOf( 
                                    S3ObjectProperty.OBJECT_ID, 
                                    BeanUtils.toMap( m_existingObjects ).keySet() ),
                                    Require.beanPropertyEquals(
                                            KeyValueObservable.KEY, 
                                            KeyValueObservable.CREATION_DATE ) ) ).toSet(), 
                                            S3ObjectProperty.OBJECT_ID );

            verifyExistingObjects( dataPolicy.getVersioning() );
            prepareForImport();
            performImport();
        }

        private boolean haveMatchingMetadata(S3ObjectOnMedia mediaObject1, S3ObjectOnMedia mediaObject2)
        {
            if (mediaObject1.getMetadata().length != mediaObject2.getMetadata().length)
            {
                return false;
            }
            for ( int i = 0; i < mediaObject1.getMetadata().length; i++)
            {
                if ( !mediaObject1.getMetadata()[i].getKey()
                        .equals(mediaObject2.getMetadata()[i].getKey() ) )
                {
                    return false;
                }
                if ( !mediaObject1.getMetadata()[i].getValue()
                        .equals(mediaObject2.getMetadata()[i].getValue() ) )
                {
                    return false;
                }
            }
            return true;
        }
        
        
        private void verifyExistingObjects( final VersioningLevel versioning )
        {
            for ( final S3Object existingObject : m_existingObjects )
            {
            	if ( VersioningLevel.NONE == versioning
            			&& !m_objectsOnMedia.get( existingObject.getName() ).containsKey(existingObject.getId() ) )
            	{
            		throw new RuntimeException( 
                            "Verification failed on object '" + existingObject.getName() + "' in bucket " 
                                    + getBucketName( existingObject.getBucketId() ) 
                                    + " due to an object version mismatch (existing version " 
                                    + existingObject.getId() + " does not match versions on media " 
                                    + m_objectsOnMedia.get( existingObject.getName() ).keySet() + ")."
                            		+ " To import this tape, you must delete the existing object"
                            		+ " or enable versioning." );
            	}            
                final S3ObjectOnMedia knownObjectOnMedia = m_objectsOnMedia.get( existingObject.getName() ).get( 
                        existingObject.getId() );
                if ( null != knownObjectOnMedia )
                {
	                for ( final Blob existingBlob : m_serviceManager.getRetriever( Blob.class ).retrieveAll(
	                        Blob.OBJECT_ID, existingObject.getId() ).toSet() )
	                {
	                    for ( final BlobOnMedia blobOnMedia : knownObjectOnMedia.getBlobs() )
	                    {
	                        if ( blobOnMedia.getOffset() != existingBlob.getByteOffset() )
	                        {
	                            continue;
	                        }
	                        validateBlobProperty( existingObject, existingBlob, blobOnMedia,
	                        		Identifiable.ID );
	                        validateBlobProperty( existingObject, existingBlob, blobOnMedia,
	                        		Blob.LENGTH );
	                        validateBlobProperty( existingObject, existingBlob, blobOnMedia,
	                        		ChecksumObservable.CHECKSUM );
	                        validateBlobProperty( existingObject, existingBlob, blobOnMedia,
	                        		ChecksumObservable.CHECKSUM_TYPE );
	                        m_existingBlobs.add( existingBlob.getId() );
	                    }
	                }
                }
            }
        }


        private void validateBlobProperty(
                final S3Object o, 
                final Blob blob, 
                final BlobOnMedia blobOnMedia,
                final String propertyName )
        {
            final Object blobValue;
            final Object blobOnMediaValue;
            try
            {
                final Method blobReader = BeanUtils.getReader( Blob.class, propertyName );
                final Method blobOnMediaReader = BeanUtils.getReader( BlobOnMedia.class, propertyName );
                blobValue = blobReader.invoke( blob );
                blobOnMediaValue = blobOnMediaReader.invoke( blobOnMedia );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }

            if ( !blobValue.equals( blobOnMediaValue ) )
            {
                throw new RuntimeException(
                        "Verification failed on object '" + o.getName() + "' in bucket " 
                        + getBucketName( o.getBucketId() )  + " since blob at offset " + blob.getByteOffset()
                        + " had a "
                        + propertyName + " value of '" + blobValue + "', but the value on media was '" 
                        + blobOnMediaValue + "'." );
            }
        }


        private void prepareForImport()
        {
            final Set< ? > existingBps =
                    m_serviceManager.getRetriever( m_blobPersistenceTargetType ).retrieveAll( Require.all(
                            m_blobPersistenceTargetFilter,
                            Require.beanPropertyEqualsOneOf( 
                                    BlobObservable.BLOB_ID, m_existingBlobs ) ) ).toSet();
            m_existingBpBlobs.addAll(
                    BeanUtils.< UUID >extractPropertyValues( existingBps, BlobObservable.BLOB_ID ) );
        }


        private void performImport()
        {
            final BeansServiceManager transaction = m_serviceManager.startTransaction();
            final BucketLogicalSizeCache bucketLogicalSizeCache = m_serviceManager.getService( BucketService.class ).getLogicalSizeCache();
            boolean createdBlobs = false;
            try
            {
                final BeansRetriever< ? > blobPersistenceRecordService =
                        transaction.getRetriever( m_blobPersistenceTargetType );
                final Method createInBulkMethod =
                        blobPersistenceRecordService.getClass().getMethod( "create", Set.class );

                for ( final Map< UUID, S3ObjectOnMedia > v : m_objectsOnMedia.values() )
                {
                    for ( final S3ObjectOnMedia objectOnMedia : v.values() )
                    {
                        processObject( objectOnMedia );
                        for ( final BlobOnMedia blobOnMedia : objectOnMedia.getBlobs() )
                        {
                            processBlob( objectOnMedia, blobOnMedia );
                            processBp( blobOnMedia.getId() );
                        }
                    }
                }

                ensureObjectPropertiesAreNew();
                transaction.getService( S3ObjectService.class ).create( m_objects );
                transaction.getService( S3ObjectPropertyService.class ).create( m_objectProperties );
                transaction.getService( BlobService.class ).create( m_blobs );
                for ( Blob blob : m_blobs )
                {
                    bucketLogicalSizeCache.blobCreated( m_bucket.getId(), blob.getLength() );
                }
                createdBlobs = true;
                final Set< UUID > completedObjectIds = processCompletelyImportedObjects( transaction );
                createInBulkMethod.invoke( blobPersistenceRecordService, m_bps );
                deleteDegradedBlobs( 
                        transaction.getService( DegradedBlobService.class ),
                        BeanUtils.< UUID >extractPropertyValues( m_bps, BlobObservable.BLOB_ID ) );
                for ( UUID objectId : completedObjectIds )
                {
                	final S3Object object = transaction.getService( S3ObjectService.class ).attain( objectId );
                	transaction.getService( S3ObjectService.class ).markObjectReceived( object );
                }
                m_completedObjectIds.addAll( completedObjectIds );
                final Set< UUID > newBlobIds = BeanUtils.toMap( m_blobs ).keySet();
                if ( !newBlobIds.isEmpty() )
                {
                    recordDegradedBlobs( newBlobIds, transaction );
                }
                transaction.commitTransaction();
            }
            catch ( final Exception ex )
            {
                if ( createdBlobs )
                {
                    for ( Blob blob : m_blobs )
                    {
                        bucketLogicalSizeCache.blobDeleted( m_bucket.getId(), blob.getLength() );
                    }
                }
                throw ExceptionUtil.toRuntimeException( ex );
            }
            finally
            {
                transaction.closeTransaction();
            }
        }


        private Set< UUID > processCompletelyImportedObjects( final BeansServiceManager transaction )
        {
            final Map< UUID, Date > completedObjects = new HashMap<>();
            for ( final Map.Entry< UUID, Integer > e : m_totalBlobCounts.entrySet() )
            {
                if ( null == e.getValue() )
                {
                    final Date creationDate = getCreationDate( transaction, e.getKey(), false );
                    if ( null != creationDate )
                    {
                        completedObjects.put( e.getKey(), creationDate );
                    }
                    continue;
                }
                final int numBlobs = 
                        transaction.getRetriever( Blob.class ).getCount( Blob.OBJECT_ID, e.getKey() );
                if ( numBlobs == e.getValue().intValue() )
                {
                    completedObjects.put( e.getKey(), getCreationDate( transaction, e.getKey(), true ) );
                }
            }

            for ( final Map.Entry< UUID, Date > e : completedObjects.entrySet() )
            {
                transaction.getService( S3ObjectService.class ).update( 
                        (S3Object)BeanFactory.newBean( S3Object.class )
                        .setCreationDate( e.getValue() ).setId( e.getKey() ), 
                        S3Object.CREATION_DATE );
            }
            transaction.getService( S3ObjectPropertyService.class ).deleteTemporaryCreationDates( 
                    completedObjects.keySet() );
            return completedObjects.keySet();
        }


        private Date getCreationDate(
                final BeansServiceManager transaction,
                final UUID objectId,
                final boolean failIfDoesNotExist )
        {
            final S3ObjectProperty op = 
                    transaction.getRetriever( S3ObjectProperty.class ).retrieve( Require.all(
                            Require.beanPropertyEquals(
                                    S3ObjectProperty.OBJECT_ID, 
                                    objectId ),
                                    Require.beanPropertyEquals( 
                                            KeyValueObservable.KEY,
                                            KeyValueObservable.CREATION_DATE ) ) );
            if ( null == op && failIfDoesNotExist )
            {
                final S3Object existingObject = 
                        m_serviceManager.getRetriever( S3Object.class ).retrieve( objectId );
                if ( null != existingObject && null != existingObject.getCreationDate() )
                {
                    return existingObject.getCreationDate();
                }
                LOG.warn( "No creation date could be determined for object " + objectId + "." );
                m_objectsWithoutCreationDate++;
            }
            return ( null == op ) ? null : new Date( Long.parseLong( op.getValue() ) );
        }


        private void ensureObjectPropertiesAreNew()
        {
            final Set< UUID > objectIds = 
                    BeanUtils.extractPropertyValues( m_objectProperties, S3ObjectProperty.OBJECT_ID );
            final Map< UUID, Set< String > > properties = new HashMap<>();
            for ( final UUID objectId : objectIds )
            {
                properties.put( objectId, new HashSet< String >() );
            }

            try ( final EnhancedIterable< S3ObjectProperty > iterable =
                    m_serviceManager.getRetriever( S3ObjectProperty.class ).retrieveAll( 
                            Require.beanPropertyEqualsOneOf( S3ObjectProperty.OBJECT_ID, objectIds ) )
                            .toIterable() )
            {
                for ( final S3ObjectProperty property : iterable )
                {
                    properties.get( property.getObjectId() ).add( property.getKey() );
                }
            }

            for ( final S3ObjectProperty property : new HashSet<>( m_objectProperties ) )
            {
                if ( properties.get( property.getObjectId() ).contains( property.getKey() ) )
                {
                    m_objectProperties.remove( property );
                }
                else
                {
                    properties.get( property.getObjectId() ).add( property.getKey() );
                }
            }
        }


        private void processObject( final S3ObjectOnMedia objectOnMedia )
        {
            final boolean mustInitializeObject = 
                    ( !m_existingObjectNames.containsKey( objectOnMedia.getObjectName() )
                            || !m_existingObjectNames.get( objectOnMedia.getObjectName() ).contains(
                                    objectOnMedia.getId() ) );
            Date creationDate = null;
            Integer totalBlobCount = null;
            for ( final S3ObjectMetadataKeyValue mediaKeyValue : objectOnMedia.getMetadata() )
            {
                if ( KeyValueObservable.CREATION_DATE.equals( mediaKeyValue.getKey() ) )
                {
                    creationDate = 
                            new Date( Long.parseLong( mediaKeyValue.getValue() ) );
                }
                else if ( KeyValueObservable.TOTAL_BLOB_COUNT.equals( mediaKeyValue.getKey() ) )
                {
                    totalBlobCount = Integer.valueOf( mediaKeyValue.getValue() );
                }
                else
                {
                    m_objectProperties.add( BeanFactory.newBean( S3ObjectProperty.class )
                            .setKey( mediaKeyValue.getKey() )
                            .setValue( mediaKeyValue.getValue() )
                            .setObjectId( objectOnMedia.getId() ) );
                }
            }

            if ( mustInitializeObject )
            {
                final S3Object object = (S3Object)BeanFactory.newBean( S3Object.class )
                        .setBucketId( m_bucket.getId() )
                        .setName( objectOnMedia.getObjectName() )
                        .setCreationDate( null )
                        .setId( objectOnMedia.getId() );
                m_objects.add( object );  
            }
            if ( null != creationDate 
                    && !m_existingCreationDates.contains( objectOnMedia.getId() ) )
            {
                m_objectProperties.add( BeanFactory.newBean( S3ObjectProperty.class )
                        .setKey( KeyValueObservable.CREATION_DATE )
                        .setObjectId( objectOnMedia.getId() )
                        .setValue( String.valueOf( creationDate.getTime() ) ) );
            }
            if ( null == totalBlobCount )
            {
                if ( null == m_noBlobCountLog )
                {
                    throw new RuntimeException( "Blob count not recorded for object being imported." );
                }
                m_noBlobCountLog.warn(
                        "Objects are being imported that do not specify their total blob count.  " 
                        + "This means that we don't know when we're done importing the object.  " 
                        + "Will assume these objects are fully imported immediately." );
            }
            m_totalBlobCounts.put( objectOnMedia.getId(), totalBlobCount );
        }


        private void processBlob( final S3ObjectOnMedia objectOnMedia, final BlobOnMedia blobOnMedia )
        {
            if ( m_existingBlobs.contains( blobOnMedia.getId() ) )
            {
                return;
            }
            if ( m_requiredChecksumType != blobOnMedia.getChecksumType() )
            {
                throw new RuntimeException(
                        "Cannot import object '" + objectOnMedia.getObjectName() 
                        + "' in bucket " + m_bucket.getName() + " since blob at offset " 
                        + blobOnMedia.getOffset() 
                        + " had a checksum of type " + blobOnMedia.getChecksumType()
                        + ", but the data policy for the bucket requires checksums of type "
                        + m_requiredChecksumType );
            }

            final Blob blob = BeanFactory.newBean( Blob.class )
                    .setObjectId( objectOnMedia.getId() )
                    .setByteOffset( blobOnMedia.getOffset() )
                    .setLength( blobOnMedia.getLength() );
            blob.setChecksumType( blobOnMedia.getChecksumType() )
            .setChecksum( blobOnMedia.getChecksum() );
            blob.setId( blobOnMedia.getId() );
            m_blobs.add( blob );
        }


        private void processBp( final UUID blobId )
        {
            m_lostBlobs.remove( blobId );
            if ( m_existingBpBlobs.contains( blobId ) )
            {
                return;
            }

            final BP bp = BeanFactory.newBean( m_blobPersistenceTargetType );
            bp.setBlobId( blobId );
            try
            {
                populateBlobPersistence( m_bucket.getId(), m_orderIndexes, bp );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
            m_bps.add( bp );
        }
        
        
        private void recordDegradedBlobs( 
                final Set< UUID > newBlobIds,
                final BeansServiceManager transaction )
        {
            final DegradedBlobParams params = new DegradedBlobParams( newBlobIds );
            
            new DegradedBlobRecorder<>(
                    params, DataPersistenceRule.class, DegradedBlob.PERSISTENCE_RULE_ID );
            new DegradedBlobRecorder<>(
                    params, AzureDataReplicationRule.class, DegradedBlob.AZURE_REPLICATION_RULE_ID );
            new DegradedBlobRecorder<>(
                    params, S3DataReplicationRule.class, DegradedBlob.S3_REPLICATION_RULE_ID );
            new DegradedBlobRecorder<>(
                    params, Ds3DataReplicationRule.class, DegradedBlob.DS3_REPLICATION_RULE_ID );

            transaction.getService( DegradedBlobService.class ).create( params.m_degradedBlobs );
        }
        
        
        private final class DegradedBlobParams
        {
            private DegradedBlobParams( final Set< UUID > newBlobIds )
            {
                m_newBlobIds = newBlobIds;
            }
            

            private final Set< UUID > m_newBlobIds;
            private final Set< DegradedBlob > m_degradedBlobs = new HashSet<>();
        } // end inner class def
        
        
        private final class DegradedBlobRecorder< R extends DatabasePersistable & DataPlacement< R > >
        {
            private DegradedBlobRecorder(
                    final DegradedBlobParams params,
                    final Class< R > ruleType,
                    final String degradedBlobRuleProperty )
            {
                final Set< R > rules =
                        m_serviceManager.getRetriever( ruleType ).retrieveAll(
                                DataPlacement.DATA_POLICY_ID, m_bucket.getDataPolicyId() ).toSet();
                final Method writer = 
                        BeanUtils.getWriter( DegradedBlob.class, degradedBlobRuleProperty );
                final Method typeGetter = BeanUtils.getReader( ruleType, "type" );
                for ( final R rule : rules )
                {
                    try
                    {
                        final Object type = typeGetter.invoke( rule );
                        if ( m_ruleId.equals( rule.getId() ) 
                                || !type.toString().equals( "PERMANENT" ) )
                        {
                            continue;
                        }
                    }
                    catch ( final Exception ex )
                    {
                        throw new RuntimeException( ex );
                    }
                    for ( final UUID newBlobId : params.m_newBlobIds )
                    {
                        final DegradedBlob degradedBlob = BeanFactory.newBean( DegradedBlob.class );
                        degradedBlob.setBlobId( newBlobId );
                        degradedBlob.setBucketId( m_bucket.getId() );
                        try
                        {
                            writer.invoke( degradedBlob, rule.getId() );
                        }
                        catch ( final Exception ex )
                        {
                            throw new RuntimeException( ex );
                        }
                        params.m_degradedBlobs.add( degradedBlob );
                    }
                }
            }
        } // end inner class def


        private final Bucket m_bucket;
        private final UUID m_ruleId;
        private final ChecksumType m_requiredChecksumType;
        private final Set< S3Object > m_existingObjects;
        private final Set< UUID > m_existingCreationDates;
        private final Map< String, Map< UUID, S3ObjectOnMedia > > m_objectsOnMedia = new HashMap<>();
        private final Set< UUID > m_existingBlobs = new HashSet<>();
        private final Set< S3ObjectProperty > m_objectProperties = new HashSet<>();
        private final Set< S3Object > m_objects = new HashSet<>();
        private final Set< Blob > m_blobs = new HashSet<>();
        private final Set< BP > m_bps = new HashSet<>();
        private final Map< String, Set< UUID > > m_existingObjectNames = new HashMap<>();
        private final Set< UUID > m_existingBpBlobs = new HashSet<>();
        private final Map< UUID, Integer > m_totalBlobCounts = new HashMap<>();
        private final Map< UUID, Integer > m_orderIndexes = new HashMap<>();
    } // end inner class def


    private Bucket attainBucket( final BucketOnMedia mediaBucket )
    {
        final Bucket existingBucket = m_serviceManager.getRetriever( Bucket.class ).retrieve( 
                Bucket.NAME, mediaBucket.getBucketName() );
        if ( null != existingBucket )
        {
            LOG.info( "Bucket '" + mediaBucket.getBucketName() 
                    + "' already exists.  Will import into it using data policy "
                    + existingBucket.getDataPolicyId() + "." );
            return existingBucket;
        }
        if ( null == m_directive.getDataPolicyId() )
        {
            throw new RuntimeException(
                    "Cannot create bucket " + mediaBucket.getBucketName() + " since " 
                    + ImportDirective.DATA_POLICY_ID + " not specified." );
        }
        if ( null == m_directive.getUserId() )
        {
            throw new RuntimeException(
                    "Cannot create bucket " + mediaBucket.getBucketName() + " since " 
                    + UserIdObservable.USER_ID + " not specified." );
        }

        LOG.info( "Bucket '" + mediaBucket.getBucketName() 
                + "' does not exist.  Will create it "
                + "using the user id and data policy id specified in the import directive." );
        final Bucket bucket = BeanFactory.newBean( Bucket.class )
                .setName( mediaBucket.getBucketName() )
                .setUserId( m_directive.getUserId() )
                .setDataPolicyId( m_directive.getDataPolicyId() );
        verifyDataPolicyHasPersistenceRules( bucket.getDataPolicyId() );
        m_serviceManager.getService( BucketService.class ).create( bucket );
        return bucket;
    }


    private void verifyDataPolicyHasPersistenceRules( final UUID dataPolicyId )
    {
        if ( 0 < m_serviceManager.getRetriever( DataPersistenceRule.class ).getCount(
                DataPlacement.DATA_POLICY_ID, dataPolicyId ) )
        {
            return;
        }

        throw new RuntimeException(
                "Data policy " + getDataPolicyName( dataPolicyId ) 
                + " does not have at least one persistence rule, so it cannot be used to create a bucket." );
    }
    
    
    /**
     * @return the data replication rule id the import shall operate under
     */
    protected abstract UUID importBucketBegun( final Bucket bucket, final BeansServiceManager brm );
    
    
    protected abstract void performFailureCleanup( final BeansServiceManager brm );
    
    
    protected abstract void populateBlobPersistence(
            final UUID bucketId,
            final Map< UUID, Integer > orderIndexes,
            final BP bp ) throws Exception;
    
    
    protected abstract void deleteDegradedBlobs(
            final DegradedBlobService service, 
            final Set< UUID > blobIds );
    
    
    final protected String getStorageDomainName( final UUID storageDomainId )
    {
        return storageDomainId + " (" + m_serviceManager.getRetriever( StorageDomain.class ).attain(
                storageDomainId ).getName() + ")";
    }


    final protected String getDataPolicyName( final UUID dataPolicyId )
    {
        return dataPolicyId + " (" + m_serviceManager.getRetriever( DataPolicy.class ).attain(
                dataPolicyId ).getName() + ")";
    }


    final protected String getBucketName( final UUID bucketId )
    {
        return bucketId + " (" + m_serviceManager.getRetriever( Bucket.class ).attain(
                bucketId ).getName() + ")";
    }


    final protected String getObjectName( final UUID objectId )
    {
        final S3Object o = m_serviceManager.getRetriever( S3Object.class ).attain( objectId );
        return o.getName() + " in bucket " + getBucketName( o.getBucketId() );
    }


    private final Map< UUID, BP > m_lostBlobs = new HashMap<>();
    private final Set< UUID > m_completedObjectIds = new HashSet<>();
    private final String m_targetDescription;
    protected final Class< BP > m_blobPersistenceTargetType;
    private final WhereClause m_blobPersistenceTargetFilter;
    private final F m_importFailedCode;
    private final F m_importIncompleteCode;
    private final ImportDirectiveService< ID > m_importDirectiveService;
    protected final ID m_directive;
    private final UUID m_idOfEntityToImport;
    protected final H m_importHandler;
    private final BeansServiceManager m_serviceManager;
    private final ThrottledLog m_noBlobCountLog;
    private int m_objectsWithoutCreationDate = 0;

    protected final static Logger LOG = Logger.getLogger( BaseImporter.class );
}
