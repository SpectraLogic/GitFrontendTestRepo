/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectType;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.DeleteS3ObjectsPreCommitListener;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService.PreviousVersions;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectFailureReason;
import com.spectralogic.s3.common.rpc.dataplanner.domain.DeleteObjectsResult;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class S3ObjectServiceImpl_Test 
{

    private void assertNotNullMod(final String message,  final Object actual) {
        assertNotNull(actual,  message);
    }

    @Test
    public void testRetrieveNumberOfObjectsInBucket()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket" ).getId();
        for ( int i = 0; i < 50; ++i )
        {
            mockDaoDriver.createObject( bucketId, String.format( "object_%d", Integer.valueOf( i ) ) );
        }
        mockDaoDriver.createObject(
                mockDaoDriver.createBucket( null, "other_test_bucket" ).getId(),
                "object_1234567890" );
        
        final S3ObjectService service = dbSupport.getServiceManager().getService( S3ObjectService.class );
        assertEquals(50,  service.retrieveNumberOfObjectsInBucket(bucketId), "Shoulda returned the number of objects in the bucket.");
    }


    @Test
    public void testRetrieveIdReturnsNullWhenNullBucketNameProvided()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createObject(
                mockDaoDriver.createBucket( null, "other_test_bucket" ).getId(),
                "object_1234567890" );
        
        final S3ObjectService service = dbSupport.getServiceManager().getService( S3ObjectService.class );
        assertNull(service.retrieveId( null, "object_1234567890", null, false ), "Shoulda returned null because the bucket id was null.");
    }


    @Test
    public void testRetrieveIdReturnsNullWhenNullObjectNameProvided()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createObject(
                mockDaoDriver.createBucket( null, "other_test_bucket" ).getId(),
                "object_1234567890" );
        
        final S3ObjectService service = dbSupport.getServiceManager().getService( S3ObjectService.class );
        assertNull(service.retrieveId( "other_test_bucket", null, null, false ), "Shoulda returned null because the object id was null.");
    }


    @Test
    public void testRetrieveIdReturnsNullWhenBucketDoesNotExist()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createObject(
                mockDaoDriver.createBucket( null, "other_test_bucket" ).getId(),
                "object_1234567890" );
        
        final S3ObjectService service = dbSupport.getServiceManager().getService( S3ObjectService.class );
        assertNull(service.retrieveId( "test_bucket", "object_1234567890", null, false ), "Shoulda returned null because the bucket did not exist.");
    }


    @Test
    public void testRetrieveIdReturnsNullWhenObjectDoesNotExist()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createObject(
                mockDaoDriver.createBucket( null, "other_test_bucket" ).getId(),
                "object_1234567890" );
        
        final S3ObjectService service = dbSupport.getServiceManager().getService( S3ObjectService.class );
        assertNull(service.retrieveId( "other_test_bucket", "object_foo", null, false ), "Shoulda returned null because the object did not exist.");
    }


    @Test
    public void testRetrieveIdReturnsIdWhenBothParametersProvided()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket" ).getId();
        UUID objectId = null;
        for ( int i = 0; i < 3; ++i )
        {
            objectId = mockDaoDriver
                    .createObject( bucketId, String.format( "object_%d", Integer.valueOf( i ) ) )
                    .getId();
        }
        mockDaoDriver.createObject(
                mockDaoDriver.createBucket( null, "other_test_bucket" ).getId(),
                "object_1234567890" );
        
        final S3ObjectService service = dbSupport.getServiceManager().getService( S3ObjectService.class );
        assertEquals(objectId, service.retrieveId( "test_bucket", "object_2", null, false ), "Shoulda retrieved the id of the last object in the bucket.");
    }


    @Test
    public void testAttainIdReturnsIdWhenBothParametersProvided()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID bucketId = mockDaoDriver.createBucket( null, "test_bucket" ).getId();
        UUID objectId = null;
        for ( int i = 0; i < 3; ++i )
        {
            objectId = mockDaoDriver
                    .createObject( bucketId, String.format( "object_%d", Integer.valueOf( i ) ) )
                    .getId();
        }
        mockDaoDriver.createObject(
                mockDaoDriver.createBucket( null, "other_test_bucket" ).getId(),
                "object_1234567890" );
        
        final S3ObjectService service = dbSupport.getServiceManager().getService( S3ObjectService.class );
        assertEquals(objectId, service.attainId( "test_bucket", "object_2", null, false ), "Shoulda retrieved the id of the last object in the bucket.");
    }


    @Test
    public void testAttainIdThrowsNotFoundWhenSomethingDoesNotExist()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        mockDaoDriver.createObject(
                mockDaoDriver.createBucket( null, "other_test_bucket" ).getId(),
                "object_1234567890" );
        
        final S3ObjectService service = dbSupport.getServiceManager().getService( S3ObjectService.class );
        TestUtil.assertThrows(
                "Shoulda thrown a not found exception because neither the bucket nor the object exist.",
                AWSFailure.NO_SUCH_OBJECT,
                new BlastContainer()
                {
                    public void test()
                        {
                            service.attainId( "test_bucket", "object_2", null, false );
                        }
                    } );
    }

    
    @Test
    public void testIsEveryBlobReceived()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID objectId = mockDaoDriver.createObject( null, "obj", -1 ).getId();
        final List< Blob > blobs = new ArrayList<>( mockDaoDriver.createBlobs( objectId, 2, 10L ) );

        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final S3ObjectService service = serviceManager.getService( S3ObjectService.class );
        final BlobService blobService = serviceManager.getService( BlobService.class );

        assertFalse(service.isEveryBlobReceived( objectId ), "Shoulda said that every blob was not received.");

        blobs.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "1234" );
        blobService.update( blobs.get( 0 ), ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );

        assertFalse(service.isEveryBlobReceived( objectId ), "Shoulda said that every blob was not received.");

        blobs.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "3421" );
        blobService.update( blobs.get( 1 ), ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );

        assertTrue(service.isEveryBlobReceived( objectId ), "Shoulda said that every blob was received.");
    }
    
    
    @Test
    public void testCreateObjectsAssignsRootFolderAsFolder()
    {
        checkCreateObjectsAssignsCorrectObjectType( "movies/", S3ObjectType.FOLDER );
    }
    
    
    @Test
    public void testCreateObjectsAssignsNestedFolderAsFolder()
    {
        checkCreateObjectsAssignsCorrectObjectType( "movies/raw/", S3ObjectType.FOLDER );
    }
    
    
    @Test
    public void testCreateObjectsAssignsRootDataAsData()
    {
        checkCreateObjectsAssignsCorrectObjectType( "movies", S3ObjectType.DATA );
    }
    
    
    @Test
    public void testCreateObjectsAssignsNestedDataAsData()
    {
        checkCreateObjectsAssignsCorrectObjectType( "movies/raw/chapter1.mov", S3ObjectType.DATA );
    }
    
    
    private static void checkCreateObjectsAssignsCorrectObjectType(
            final String objectName,
            final S3ObjectType objectType )
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final User user = mockDaoDriver.createUser( "user1" );
        final Bucket bucket = mockDaoDriver.createBucket( user.getId(), "bucket1" );
        
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        final S3ObjectService service = transaction.getService( S3ObjectService.class );
        
        final S3Object object = BeanFactory.newBean( S3Object.class );
        object.setName( objectName );
        object.setBucketId( bucket.getId() );
        service.create( CollectionFactory.toSet( object ) );
        assertEquals(objectType, object.getType(), "Shoulda recognized object as the correct type.");

        transaction.closeTransaction();
    }
    
    
    @Test
    public void testCreateWhenFolderPathSameAsObjectAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final UUID bucketId = new MockDaoDriver( dbSupport )
                .createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME )
                .getId();
        createObject( dbSupport, bucketId, "object" );
        createObject( dbSupport, bucketId, "object/" );
        createObject( dbSupport, bucketId, "object/foo" );
        createObject( dbSupport, bucketId, "object/foo/" );
        createObject( dbSupport, bucketId, "objectfoo" );
        createObject( dbSupport, bucketId, "objectbar/" );
        createObject( dbSupport, bucketId, "folder/object" );
        createObject( dbSupport, bucketId, "folder/object/" );
        createObject( dbSupport, bucketId, "folder/object/baz" );
        createObject( dbSupport, bucketId, "folder/object/baz/" );
        createObject( dbSupport, bucketId, "folder/objectfoo" );
        createObject( dbSupport, bucketId, "folder/objectbar/" );

        createObjects( dbSupport, bucketId, "object", "object/" );
        createObjects( dbSupport, bucketId, "object", "object/foo" );
        createObjects( dbSupport, bucketId, "object", "object/foo/" );
    
        createObjects( dbSupport, bucketId, "folder/object", "folder/object/" );
        createObjects( dbSupport, bucketId, "folder/object", "folder/object/baz" );
        createObjects( dbSupport, bucketId, "folder/object", "folder/object/baz/" );

        createObjects(
                dbSupport,
                bucketId,
                "object",
                "objectfoo",
                "objectbar/",
                "folder/object",
                "folder/objectfoo",
                "folder/objectbar/" );
    }
    
    
    @Test
    public void testCreateWhenObjectSameAsFolderPathAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final UUID bucketId = new MockDaoDriver( dbSupport )
                .createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME )
                .getId();
        createObject( dbSupport, bucketId, "folder/" );
        createObject( dbSupport, bucketId, "folder" );
        createObject( dbSupport, bucketId, "folder/object" );
        createObject( dbSupport, bucketId, "folder2/object/" );
        createObject( dbSupport, bucketId, "folder2/object" );
        createObject( dbSupport, bucketId, "folder2" );
        createObject( dbSupport, bucketId, "folder2/" );
        createObject( dbSupport, bucketId, "folder2/object/foo" );
    
        createObjects( dbSupport, bucketId, "folder/", "folder" );
        createObjects( dbSupport, bucketId, "folder2/object/", "folder2/object" );
        createObjects( dbSupport, bucketId, "folder2/object/", "folder2" );

        createObjects(
                dbSupport,
                bucketId,
                "folder/",
                "folder/object",
                "folder2/object/",
                "folder2/",
                "folder2/object/foo" );
    }
    
    
    @Test
    public void testCreateWhenConflictingObjectNameNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final UUID bucketId = new MockDaoDriver( dbSupport )
                .createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME )
                .getId();
        createObject( dbSupport, bucketId, "foo" );
        checkCreateObjectFails( dbSupport, bucketId, "foo" );
        createObject( dbSupport, bucketId, "fid/bar" );
        checkCreateObjectFails( dbSupport, bucketId, "fid/bar" );

        createObjects(
                dbSupport,
                bucketId,
                "folder/",
                "folder/object",
                "folder2/object/",
                "folder2/",
                "folder2/object/foo" );
    }
    
    
    @Test
    public void testCreateWithReplicateObjectCreationModeLatestDeterminedCorrectly()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID bucketId = mockDaoDriver
                .createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME )
                .getId();
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucketId );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_MULTIPLE_VERSIONS ),
                DataPolicy.VERSIONING );
        final S3Object o1 = mockDaoDriver.createObject( bucketId, "foo", new Date() );
        assertTrue(mockDaoDriver.attain( S3Object.class, o1 ).isLatest(), "Shoulda initialized latest=true.");

        mockDaoDriver.updateBean( o1.setCreationDate( new Date( 3000 ) ), S3Object.CREATION_DATE );
        final S3Object o2 = mockDaoDriver.createObject( bucketId, "foo", new Date( 2000 ) );
        assertTrue(mockDaoDriver.attain( S3Object.class, o1 ).isLatest(), "Shoulda accepted newest creation date object as the latest.");
        assertFalse(mockDaoDriver.attain( S3Object.class, o2 ).isLatest(), "Shoulda accepted newest creation date object as the latest.");

        final S3Object o2b = mockDaoDriver.createObjectStub( bucketId, "foo", 10 );
        assertTrue(mockDaoDriver.attain( S3Object.class, o1 ).isLatest(), "Shoulda kept o1 as latest since it has the latest creation date.");
        assertFalse(mockDaoDriver.attain( S3Object.class, o2b ).isLatest(), "Shoulda kept o1 as latest since it has the latest creation date.");
        assertFalse(mockDaoDriver.attain( S3Object.class, o2 ).isLatest(), "Shoulda kept o1 as latest since it has the latest creation date.");

        dbSupport.getServiceManager().getService( S3ObjectService.class )
        	.delete(PreviousVersions.DELETE_SPECIFIC_VERSION, CollectionFactory.toSet( o1.getId() ) );

        final Object expected1 = o2.getId();
        assertEquals(expected1, dbSupport.getServiceManager().getRetriever( S3Object.class )
                    .retrieveAll( Require.beanPropertyEquals( S3Object.LATEST, true) ).getFirst().getId(), "Shoulda rolled back to previous latest");

        assertEquals(2,  dbSupport.getServiceManager().getRetriever(S3Object.class)
                .getCount(), "Previous two versions should still exist.");

        mockDaoDriver.updateBean( o2.setCreationDate( new Date( 2000 ) ), S3Object.CREATION_DATE );
        S3Object o3 = createObject( 
               dbSupport, bucketId, "foo", new Date( 1000 ) );
        final Object expected = o2.getId();
        assertEquals(expected, dbSupport.getServiceManager().getRetriever( S3Object.class )
                    .retrieveAll( Require.beanPropertyEquals( S3Object.LATEST, true) ).getFirst().getId(), "o2 should still be latest because of creaion date");

        dbSupport.getServiceManager().getService( S3ObjectService.class )
    	.delete(PreviousVersions.DELETE_SPECIFIC_VERSION, CollectionFactory.toSet( o3.getId() ) );
        o3 = mockDaoDriver.createObject( bucketId, "foo", new Date( 4000 ) );
        assertTrue(mockDaoDriver.attain( S3Object.class, o3 ).isLatest(), "Shoulda made o3 latest since it was created more recently than the previous latest.");
        assertFalse(mockDaoDriver.attain( S3Object.class, o2 ).isLatest(), "Shoulda made o3 latest since it was created more recently than the previous latest.");
    }
    
    
    @Test
    public void testCreateWhenConflictingObjectNameAllowedIfDataPolicyConfiguredForVersioningKeepLatest()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID bucketId = mockDaoDriver
                .createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME )
                .getId();
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucketId );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        mockDaoDriver.createObject( bucketId, "foo", new Date( 1000 ) );
        mockDaoDriver.createObject( bucketId, "foo", new Date( 2000 ) );
        mockDaoDriver.createObject( bucketId, "zulu", new Date( 2000 ) );
        final S3Object latestFoo = mockDaoDriver.createObject( bucketId, "foo", new Date(3000) );
        mockDaoDriver.createObjectStub( bucketId, "zuup", 10);

        final List< S3Object > objects = new ArrayList<>( BeanUtils.sort( 
                dbSupport.getServiceManager().getRetriever( S3Object.class )
                    .retrieveAll( Require.beanPropertyEquals( S3Object.NAME, "foo" ) ).toSet() ) );
        final S3Object latest = objects.get( 0 );
        final S3Object legacy = objects.get( 1 );
        final S3Object original = objects.get( 2 );
        assertFalse(original.isLatest(), "Shoulda reported only latest as being the latest.");
        assertFalse(legacy.isLatest(), "Shoulda reported only latest as being the latest.");
        assertTrue(latest.isLatest(), "Shoulda reported only latest as being the latest.");

        final Object expected = latest.getId();
        assertEquals(expected, latestFoo.getId(), "Latest foo shoulda been the most recent creation date foo.");

        assertEquals(2,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(
                S3Object.LATEST, Boolean.TRUE), "Shoulda been only one latest per object name.");
    }
    
    
    @Test
    public void testCreateWhenDuplicateObjectNameInCreateRequestNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final UUID bucketId = new MockDaoDriver( dbSupport )
                .createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME )
                .getId();

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                createObjects(
                        dbSupport,
                        bucketId,
                        "folder/",
                        "folder2/object/foo",
                        "folder/object",
                        "folder2/object/",
                        "folder2/",
                        "folder2/object/foo" );
            }
        } );
    }
    
    
    @Test
    public void testCreateWhenDuplicateFolderNameInCreateRequestNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final UUID bucketId = new MockDaoDriver( dbSupport )
                .createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME )
                .getId();
    
        createObjects( dbSupport, bucketId, "folder/", "folder2", "folder/object", "folder2/object/", "folder2/",
                "folder2/object/foo" );
    }
    
    
    @Test
    public void testCreateWhenObjectAndFolderNameConflictInCreateRequestNotAllowed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final UUID bucketId = new MockDaoDriver( dbSupport )
                .createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME )
                .getId();

        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                createObjects(
                        dbSupport,
                        bucketId,
                        "folder/",
                        "folder2/",
                        "folder/object",
                        "folder2/object/",
                        "folder2/",
                        "folder2/object/foo" );
            }
        } );
    }
    
    
    @Test
    public void testCreateDuplicateObjectOrFolderNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final UUID bucketId = new MockDaoDriver( dbSupport )
                .createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME )
                .getId();
        createObject( dbSupport, bucketId, "object" );
        checkCreateObjectFails( dbSupport, bucketId, "object" );
        createObject( dbSupport, bucketId, "folder/object" );
        checkCreateObjectFails( dbSupport, bucketId, "folder/object" );
        createObject( dbSupport, bucketId, "directory/" );
        checkCreateObjectFails( dbSupport, bucketId, "directory/" );

        checkCreateObjectsFails( dbSupport, bucketId, "object", "object" );
        checkCreateObjectsFails( dbSupport, bucketId, "folder/object", "folder/object" );
        checkCreateObjectsFails( dbSupport, bucketId, "directory/", "directory/" );
    }
    
    
    @Test
    public void testCreateWithEmptyFolderNamesAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final UUID bucketId = new MockDaoDriver( dbSupport )
                .createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME )
                .getId();
        createObjects( dbSupport, bucketId, "/" );
        createObjects( dbSupport, bucketId, "/foo" );
        createObjects( dbSupport, bucketId, "/foo/" );
        createObjects( dbSupport, bucketId, "/foo/bar" );
        createObjects( dbSupport, bucketId, "/foo/bar/" );
        createObjects( dbSupport, bucketId, "foo//" );
        createObjects( dbSupport, bucketId, "foo//bar" );
        createObjects( dbSupport, bucketId, "foo/bar//" );
    }


    private static void checkCreateObjectFails(
            final DatabaseSupport dbSupport,
            final UUID bucketId,
            final String objectName )
    {
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
                {
                    createObject( dbSupport, bucketId, objectName );
                }
            } );
    }


    private static void createObject(
            final DatabaseSupport dbSupport,
            final UUID bucketId,
            final String objectName )
    {
        createObject( dbSupport, bucketId, objectName, new Date() );
    }


    private static S3Object createObject(
            final DatabaseSupport dbSupport,
            final UUID bucketId,
            final String objectName,
            final Date creationDate )
    {
        final S3Object object = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucketId )
                .setName( objectName )
                .setCreationDate( creationDate );
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( S3ObjectService.class ).create( 
                    CollectionFactory.toSet( object ) );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        return object;
    }


    private static void checkCreateObjectsFails(
            final DatabaseSupport dbSupport,
            final UUID bucketId,
            final String... objectNames )
    {
        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
                {
                    createObjects( dbSupport, bucketId, objectNames );
                }
            } );
    }


    private static void createObjects(
            final DatabaseSupport dbSupport,
            final UUID bucketId,
            final String... objectNames )
    {
        dbSupport.getDataManager().deleteBeans( S3Object.class, Require.nothing() );

        final Set< S3Object > objects = new HashSet<>();
        for ( final String objectName : objectNames )
        {
            objects.add( BeanFactory.newBean( S3Object.class )
                        .setBucketId( bucketId )
                        .setName( objectName ) );
        }

        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( S3ObjectService.class ).create( objects );
        }
        finally
        {
            transaction.closeTransaction();
        }
    }
    
    
    @Test
    public void testCreateObjectsEmptySetOfObjectsDoesNothing()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        try
        {
            transaction.getService( S3ObjectService.class ).create( new HashSet< S3Object >() );
        }
        finally
        {
            transaction.closeTransaction();
        }
    }
    
    @Test
    public void testCreateObjectsAcrossDifferentBucketsNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final BeansServiceManager transaction =
                dbSupport.getServiceManager().startTransaction();
        final S3ObjectService service = transaction.getService( S3ObjectService.class );

        final User user =
                BeanFactory.newBean( User.class ).setName( "myUser" )
                .setAuthId( "myAuthId" ).setSecretKey( "mySecretKey" );
        dbSupport.getDataManager().createBean( user );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );

        final S3Object o1 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket.getId() ).setName( "a" );
        final S3Object o2 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket2.getId() ).setName( "b" );

        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
                {
                    service.create( CollectionFactory.toSet( o1, o2 ) );
                }
            } );

        o2.setBucketId( bucket.getId() );
        service.create( CollectionFactory.toSet( o1, o2 ) );
        transaction.closeTransaction();
    }
    
    
    @Test
    public void testDuplicateNamesFailOnlyWhenVersioningNone()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final BeansServiceManager transaction =
                dbSupport.getServiceManager().startTransaction();
        final S3ObjectService service = transaction.getService( S3ObjectService.class );

        final User user =
                BeanFactory.newBean( User.class ).setName( "myUser" )
                .setAuthId( "myAuthId" ).setSecretKey( "mySecretKey" );
        dbSupport.getDataManager().createBean( user );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dataPolicy" );
        mockDaoDriver.updateBean( dataPolicy.setVersioning( VersioningLevel.NONE ), DataPolicy.VERSIONING );
                
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), "bucket1" );

        final S3Object o1 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket.getId() ).setName( "a" );
        final S3Object o2 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket.getId() ).setName( "a" );

        TestUtil.assertThrows( null, DaoException.class, new BlastContainer()
        {
            public void test()
                {
                    service.create( CollectionFactory.toSet( o1, o2 ) );
                }
            } );

        mockDaoDriver.updateBean( dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ), DataPolicy.VERSIONING );
        service.create( CollectionFactory.toSet( o1, o2 ) );
        transaction.closeTransaction();
    }
    
    
    @Test
    public void testCreateObjectsWithNamingCollisionNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final BeansServiceManager transaction = 
                dbSupport.getServiceManager().startTransaction();
        final S3ObjectService service = transaction.getService( S3ObjectService.class );
        
        final User user =
                BeanFactory.newBean( User.class ).setName( "myUser" )
                .setAuthId( "myAuthId" ).setSecretKey( "mySecretKey" );
        dbSupport.getDataManager().createBean( user );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        mockDaoDriver.createBucket( null, "bucket2" );
        
        final S3Object o1 = BeanFactory.newBean( S3Object.class ).setBucketId( bucket.getId() )
                .setName( "a" );
        final S3Object o2 = BeanFactory.newBean( S3Object.class ).setBucketId( bucket.getId() )
                .setName( "a" + S3Object.DELIMITER );
        service.create(CollectionFactory.toSet( o1 ) );
        service.create( CollectionFactory.toSet( o2 ) );
        
        transaction.closeTransaction();
    }
    
    
    @Test
    public void testCreateObjectsWithNamingCollisionWithinBulkRequestNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final BeansServiceManager transaction = 
                dbSupport.getServiceManager().startTransaction();
        final S3ObjectService service = transaction.getService( S3ObjectService.class );

        final User user =
                BeanFactory.newBean( User.class ).setName( "myUser" )
                .setAuthId( "myAuthId" ).setSecretKey( "mySecretKey" );
        dbSupport.getDataManager().createBean( user );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        mockDaoDriver.createBucket( null, "bucket2" );
        
        final S3Object o1 = BeanFactory.newBean( S3Object.class ).setBucketId( bucket.getId() )
                .setName( "a" );
        final S3Object o2 = BeanFactory.newBean( S3Object.class ).setBucketId( bucket.getId() )
                .setName( "a" + S3Object.DELIMITER );
    
        service.create( CollectionFactory.toSet( o1, o2 ) );
        transaction.closeTransaction();
    }
    
    
    @Test
    public void testGetSizeInBytesReturnsSizeInBytes()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final BeansServiceManager transaction = 
                dbSupport.getServiceManager().startTransaction();
        final S3ObjectService service = transaction.getService( S3ObjectService.class );

        final User user =
                BeanFactory.newBean( User.class ).setName( "myUser" )
                .setAuthId( "myAuthId" ).setSecretKey( "mySecretKey" );
        dbSupport.getDataManager().createBean( user );
        final Bucket bucket = mockDaoDriver.createBucket( null, "bucket1" );
        mockDaoDriver.createBucket( null, "bucket2" );
        
        final S3Object o1 = BeanFactory.newBean( S3Object.class ).setBucketId( bucket.getId() )
                .setName( "a" );
        o1.setId( UUID.randomUUID() );
        final Blob b1 = BeanFactory.newBean( Blob.class )
                .setByteOffset( 0 ).setLength( 99 ).setObjectId( o1.getId() );
        final Blob b2 = BeanFactory.newBean( Blob.class )
                .setByteOffset( 99 ).setLength( 900 ).setObjectId( o1.getId() );
        final Set< S3Object > objects = new HashSet<>();
        final Set< Blob > blobs = new HashSet<>();
        for ( int i = 0; i < 100; ++i )
        {
            final S3Object o = BeanFactory.newBean( S3Object.class ).setBucketId( bucket.getId() )
                    .setName( "object" + i );
            o.setId( UUID.randomUUID() );
            objects.add( o );
            blobs.add( BeanFactory.newBean( Blob.class )
                    .setObjectId( o.getId() ).setByteOffset( 0 ).setLength( 1 ) );
        }
        service.create(objects );
        service.create(CollectionFactory.toSet( o1 ) ); 
        transaction.getService( BlobService.class ).create( CollectionFactory.toSet( b1, b2 ) );
        transaction.getService( BlobService.class ).create( blobs );

        assertEquals(
                objects.size(),
                service.getSizeInBytes( BeanUtils.toMap( objects ).keySet() ),
                "Shoulda returned size in bytes."
               );

        transaction.closeTransaction();
    }
    
    
    @Test
    public void testDeleteByBucketIdDeletesAllS3ObjectsAndLinkedTypes()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final UUID bucketId = mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME ).getId();
        mockDaoDriver.createObject( bucketId, "foo1", 50L );
        mockDaoDriver.createObject( bucketId, "foo/" );
        mockDaoDriver.createObject( bucketId, "foo/bar1", 1024L );
        final UUID multiBlobObjectId = mockDaoDriver.createObject( bucketId, "foo/baz1", -1 ).getId();
        final List< Blob > blobs = mockDaoDriver.createBlobs( multiBlobObjectId, 12, 123L );
        final UUID jobId = mockDaoDriver.createJob( bucketId, null, JobRequestType.GET ).getId();
        mockDaoDriver.createJobEntries(jobId, blobs );
        final Map< String, String > propertiesMapping = new HashMap<>();
        propertiesMapping.put( "x-amz-meta-foo", "bar" );
        mockDaoDriver.createObjectProperties( multiBlobObjectId, propertiesMapping  );
        
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final S3ObjectService objectService = serviceManager.getService( S3ObjectService.class );

        objectService.deleteByBucketId( bucketId );

        assertEquals(0,  objectService.getCount(), "Should notta had any objects in the data store.");
        assertEquals(0,  serviceManager.getService(BlobService.class).getCount(), "Should notta had any blobs in the data store.");
        assertEquals(0,  serviceManager.getService(JobEntryService.class).getCount(), "Should notta had any job entries in the data store.");
        assertEquals(0,  serviceManager.getService(S3ObjectPropertyService.class).getCount(), "Should notta had any object properties in the data store.");
    }
    
    
    @Test
    public void testGetLockReturnsSameLockRegardlessAsToWhetherInsideTransaction()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        assertSame(
                dbSupport.getServiceManager().getService( S3ObjectService.class ).getLock(),
                transaction.getService( S3ObjectService.class ).getLock(),
                "Transaction shoulda delegated to source to get the lock instance."
                 );
        transaction.closeTransaction();
    }
    
    
    @Test
    public void testObjectsEntirelyPersistedWhacksPreviousVersionsOfObjectsWhenPossibleAndVeryLargeDataSet()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Object baseObject = mockDaoDriver.createObject( null, "base" );
        baseObject.setCreationDate( new Date() );
        final Set< S3Object > objects = new HashSet<>();
        
        final Bucket b = mockDaoDriver.createBucket( null, "bucket" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( b.getId() );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        for ( int i = 0; i < 1000; ++i )
        {
            if ( 0 == i % 100 )
            {
                mockDaoDriver.createObject( b.getId(), "o" + i );
            }
            objects.add( mockDaoDriver.createObject( b.getId(), "o" + i ) );
        }
        assertEquals(1011,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(), "Should notta whacked anything yet.");

        final S3ObjectService deleter = dbSupport.getServiceManager().getService( S3ObjectService.class );
        deleter.deleteLegacyObjectsIfEntirelyPersisted(
                BeanUtils.extractPropertyValues( objects, Identifiable.ID ) );
        assertEquals(1001,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(), "Shoulda whacked everything except base object and blob.");

        deleter.deleteLegacyObjectsIfEntirelyPersisted(
                BeanUtils.extractPropertyValues( objects, Identifiable.ID ) );
        assertEquals(1001,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(), "Should notta whacked anything extra.");
    }
    
    
    @Test
    public void testDeleteLegacyObjectsDoesNotWhackPreviousVersionsOfObjectsWhenObjectNotFullyPersisted()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dp = mockDaoDriver.createABMConfigSingleCopyOnTape();
        final Bucket bucket = mockDaoDriver.createBucket( null, dp.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        final Tape tape = mockDaoDriver.createTape();
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        final S3Object o1v1 = mockDaoDriver.createObject( bucket.getId(), "o1", 10 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1v1.getId() );
        final S3Object o1v2 = mockDaoDriver.createObject( bucket.getId(), "o1", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o1v2.getId() );
        final S3Object o1v3 = mockDaoDriver.createObject( bucket.getId(), "o1", -1 );
        final List< Blob > b3 = mockDaoDriver.createBlobs( o1v3.getId(), 2, 10 );
        final S3Object o2v1 = mockDaoDriver.createObject( bucket.getId(), "o2", 10 );
        final Blob b4 = mockDaoDriver.getBlobFor( o2v1.getId() );
        final S3Object o2v2 = mockDaoDriver.createObject( bucket.getId(), "o2", 10 );
        final Blob b5 = mockDaoDriver.getBlobFor( o2v2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3" );
        final Blob b6 = mockDaoDriver.getBlobFor( o3.getId() );

        final S3ObjectService deleter = dbSupport.getServiceManager().getService( S3ObjectService.class );
        deleter.deleteLegacyObjectsIfEntirelyPersisted( CollectionFactory.toSet( o1v3.getId() ) );

        assertEquals(CollectionFactory.toSet( b1.getId(), b2.getId(),
                		b3.get( 0 ).getId(), b3.get( 1 ).getId(), b4.getId(), b5.getId(), b6.getId() ).size(),  BeanUtils.toMap(dbSupport.getServiceManager()
                .getRetriever(Blob.class).retrieveAll().toSet()).keySet().size(), "Should notta deleted anything yet because nothing is persisted yet.");

        mockDaoDriver.putBlobOnTapeAndDetermineStorageDomain( tape.getId(), b1.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), b2.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), b3.get(0).getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), b3.get(1).getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), b4.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), b5.getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), b6.getId() );

        assertEquals(CollectionFactory.toSet( b1.getId(), b2.getId(),
                		b3.get( 0 ).getId(), b3.get( 1 ).getId(), b4.getId(), b5.getId(), b6.getId() ).size(),  BeanUtils.toMap(dbSupport.getServiceManager()
                .getRetriever(Blob.class).retrieveAll().toSet()).keySet().size(), "Should notta deleted anything yet.");

        deleter.deleteLegacyObjectsIfEntirelyPersisted( CollectionFactory.toSet( o1v3.getId() ) );

        final Object expected = CollectionFactory.toSet(
                b3.get( 0 ).getId(), b3.get( 1 ).getId(), b4.getId(), b5.getId(), b6.getId() );
        assertEquals(expected, BeanUtils.toMap( dbSupport.getServiceManager()
                		.getRetriever( Blob.class ).retrieveAll().toSet() ).keySet(), "Shoulda whacked previous versions of o1 only.");
    }
    
    
    @Test
    public void testDeleteLegacyObjectsDoesNotWhackPreviousVersionsOfObjectsWhenObjectHasNoCreationDate()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        final S3Object o1v1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", 10 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1v1.getId() );
        final S3Object o1v2 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o1v2.getId() );
        final S3Object o1v3 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", 10 );
        final Blob b3 = mockDaoDriver.getBlobFor( o1v3.getId() );
        final S3Object o2v1 = mockDaoDriver.createObjectStub( bucket.getId(), "o2", 10 );
        final Blob b4 = mockDaoDriver.getBlobFor( o2v1.getId() );
        final S3Object o2v2 = mockDaoDriver.createObjectStub( bucket.getId(), "o2", 10 );
        final Blob b5 = mockDaoDriver.getBlobFor( o2v2.getId() );
        final S3Object o3 = mockDaoDriver.createObjectStub( bucket.getId(), "o3", 10 );
        final Blob b6 = mockDaoDriver.getBlobFor( o3.getId() );

        final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.createJobEntry( job.getId(), b3 );

        final S3ObjectService deleter = dbSupport.getServiceManager().getService( S3ObjectService.class );
        deleter.deleteLegacyObjectsIfEntirelyPersisted( CollectionFactory.toSet( o1v3.getId() ) );
        final Object expected = CollectionFactory.toSet(
         b1.getId(), b2.getId(), b3.getId(), b4.getId(), b5.getId(), b6.getId() );
        assertEquals(expected, BeanUtils.toMap( 
                 dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet() ).keySet(), "Shoulda whacked previous versions of o1 only.");
    }
    
    @Test
    public void testDeleteLegacyObjectsDoesNotFailWhenMultipleVersionsOfSameObjectPassed()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        final S3Object o1v1 = mockDaoDriver.createObject( bucket.getId(), "o1", 10 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1v1.getId() );
        final S3Object o1v2 = mockDaoDriver.createObject( bucket.getId(), "o1", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o1v2.getId() );
        final S3Object o1v3 = mockDaoDriver.createObject( bucket.getId(), "o1", 10 );
        final Blob b3 = mockDaoDriver.getBlobFor( o1v3.getId() );

        /*final Job job = mockDaoDriver.createJob( null, null, JobRequestType.PUT );
        mockDaoDriver.createJobChunk( job.getId(), CollectionFactory.toSet( b3 ) )*/;

        final S3ObjectService deleter = dbSupport.getServiceManager().getService( S3ObjectService.class );
        deleter.deleteLegacyObjectsIfEntirelyPersisted( CollectionFactory.toSet( o1v1.getId(), o1v2.getId(), o1v3.getId() ) );
        final Object expected = CollectionFactory.toSet(
        b3.getId());
        assertEquals(expected, BeanUtils.toMap( 
                 dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet() ).keySet(), "Shoulda whacked older versions of o1.");
    }
    
    
    @Test
    public void testDeleteWithRollbackToPreviousVersionDoesSoWhenPossible()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        final S3Object o1v1 = mockDaoDriver.createObject( bucket.getId(), "o1", 10, new Date( 1000 ) );
        final Blob b1 = mockDaoDriver.getBlobFor( o1v1.getId() );
        final S3Object o1v2 = mockDaoDriver.createObject( bucket.getId(), "o1", 10, new Date( 2000 )  );
        mockDaoDriver.getBlobFor( o1v2.getId() );
        final S3Object o1v3 = mockDaoDriver.createObject( bucket.getId(), "o1", 10, new Date( 3000 )  );
        final Blob b3 = mockDaoDriver.getBlobFor( o1v3.getId() );
        final S3Object o2v1 = mockDaoDriver.createObject( bucket.getId(), "o2", 10 );
        final Blob b4 = mockDaoDriver.getBlobFor( o2v1.getId() );
        final S3Object o2v2 = mockDaoDriver.createObject( bucket.getId(), "o2", 10 );
        final Blob b5 = mockDaoDriver.getBlobFor( o2v2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3" );
        final Blob b6 = mockDaoDriver.getBlobFor( o3.getId() );
        final S3ObjectService service = dbSupport.getServiceManager().getService( S3ObjectService.class );
        service.delete( PreviousVersions.DELETE_SPECIFIC_VERSION, CollectionFactory.toSet( o1v2.getId() ) );
        final Object expected2 = CollectionFactory.toSet(
         b1.getId(), b3.getId(), b4.getId(), b5.getId(), b6.getId() );
        assertEquals(expected2, BeanUtils.toMap( 
                 dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet() ).keySet(), "Shoulda whacked o1v2.");
        assertEquals(false, service.attain( o1v1.getId() ).isLatest(), "Should notta made o1v1 the latest.");

        service.delete( PreviousVersions.DELETE_SPECIFIC_VERSION, CollectionFactory.toSet( o1v3.getId() ) );
        final Object expected1 = CollectionFactory.toSet(
         b1.getId(), b4.getId(), b5.getId(), b6.getId() );
        assertEquals(expected1, BeanUtils.toMap( 
                 dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet() ).keySet(), "Shoulda whacked o1v3.");
        assertEquals(true, service.attain( o1v1.getId() ).isLatest(), "Shoulda made o1v1 the latest.");

        service.delete( PreviousVersions.DELETE_SPECIFIC_VERSION, CollectionFactory.toSet( o1v1.getId() ) );
        final Object expected = CollectionFactory.toSet(
         b4.getId(), b5.getId(), b6.getId() );
        assertEquals(expected, BeanUtils.toMap( 
                 dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet() ).keySet(), "Shoulda whacked o1v1.");
    }
    
    
    @Test
    public void testDeleteSpecificAndDeleteAllModesHonored()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        final S3Object o1v1 = mockDaoDriver.createObject( bucket.getId(), "o1", 10, new Date( 1000 ) );
        final Blob b1 = mockDaoDriver.getBlobFor( o1v1.getId() );
        final S3Object o1v2 = mockDaoDriver.createObject( bucket.getId(), "o1", 10, new Date( 2000 ) );
        final Blob b2 = mockDaoDriver.getBlobFor( o1v2.getId() );
        final S3Object o1v3 = mockDaoDriver.createObject( bucket.getId(), "o1", 10, new Date( 3000 ) );
        final Blob b3 = mockDaoDriver.getBlobFor( o1v3.getId() );
        final S3Object o2v1 = mockDaoDriver.createObject( bucket.getId(), "o2", 10 );
        final Blob b4 = mockDaoDriver.getBlobFor( o2v1.getId() );
        final S3Object o2v2 = mockDaoDriver.createObject( bucket.getId(), "o2", 10 );
        final Blob b5 = mockDaoDriver.getBlobFor( o2v2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3" );
        final Blob b6 = mockDaoDriver.getBlobFor( o3.getId() );
        
        final S3ObjectService service = dbSupport.getServiceManager().getService( S3ObjectService.class );
        DeleteObjectsResult result = service.delete(
                PreviousVersions.DELETE_SPECIFIC_VERSION, CollectionFactory.toSet( o1v2.getId() ) ).toDeleteObjectsResult();
        assertTrue(result.isDaoModified(), "Shoulda reported dao modified.");
        final Object expected1 = CollectionFactory.toSet(
         b1.getId(), b3.getId(), b4.getId(), b5.getId(), b6.getId() );
        assertEquals(expected1, BeanUtils.toMap( 
                 dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet() ).keySet(), "Shoulda whacked o1v2.");

        result = service.delete(
                PreviousVersions.DELETE_ALL_VERSIONS, CollectionFactory.toSet( o1v3.getId() ) ).toDeleteObjectsResult();
        assertTrue(result.isDaoModified(), "Shoulda reported dao modified.");
        final Object expected = CollectionFactory.toSet(
         b4.getId(), b5.getId(), b6.getId() );
        assertEquals(expected, BeanUtils.toMap( 
                 dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet() ).keySet(), "Shoulda whacked o1v1 and o1v3.");

        result = service.delete(
                PreviousVersions.DELETE_ALL_VERSIONS, CollectionFactory.toSet( o1v3.getId() ) ).toDeleteObjectsResult();
        assertFalse(result.isDaoModified(), "Shoulda reported dao not modified.");
    }
    
    
    @Test
    public void testUnmarkLatestModeHonored()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, MockDaoDriver.DEFAULT_BUCKET_NAME );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucket.getId() );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        final S3Object o1v1 = mockDaoDriver.createObject( bucket.getId(), "o1", 10, new Date( 1000 ) );
        final Blob b1 = mockDaoDriver.getBlobFor( o1v1.getId() );
        final S3Object o1v2 = mockDaoDriver.createObject( bucket.getId(), "o1", 10, new Date( 2000 ) );
        final Blob b2 = mockDaoDriver.getBlobFor( o1v2.getId() );
        final S3Object o1v3 = mockDaoDriver.createObject( bucket.getId(), "o1", 10, new Date( 3000 ) );
        final Blob b3 = mockDaoDriver.getBlobFor( o1v3.getId() );
        final S3Object o2v1 = mockDaoDriver.createObject( bucket.getId(), "o2", 10, new Date( 1000 ) );
        final Blob b4 = mockDaoDriver.getBlobFor( o2v1.getId() );
        final S3Object o2v2 = mockDaoDriver.createObject( bucket.getId(), "o2", 10, new Date( 2000 ));
        final Blob b5 = mockDaoDriver.getBlobFor( o2v2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3", new Date( 2000 ) );
        final Blob b6 = mockDaoDriver.getBlobFor( o3.getId() );
        
        final S3ObjectService service = dbSupport.getServiceManager().getService( S3ObjectService.class );
        DeleteObjectsResult result = service.delete(
                PreviousVersions.UNMARK_LATEST, CollectionFactory.toSet( o1v2.getId()) ).toDeleteObjectsResult();
        final Object expected5 = CollectionFactory.toSet(
         b1.getId(), b2.getId(), b3.getId(), b4.getId(), b5.getId(), b6.getId() );
        assertEquals(expected5, BeanUtils.toMap( 
                 dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet() ).keySet(), "Should notta removed anything.");

        assertEquals(3,  dbSupport.getServiceManager()
                .getRetriever(S3Object.class).getCount(Require.beanPropertyEquals(S3Object.LATEST, true)), "Number of latest objects should be 3");
        assertTrue(mockDaoDriver.attain(S3Object.class, o1v3).isLatest(), "o1v3 should be latest");
        assertTrue(mockDaoDriver.attain(S3Object.class, o2v2).isLatest(), "o2v2 should be latest");
        assertTrue(mockDaoDriver.attain(S3Object.class, o3).isLatest(), "o3 should still be latest");

        result = service.delete(
                PreviousVersions.UNMARK_LATEST, CollectionFactory.toSet( o1v3.getId(), o2v2.getId() ) ).toDeleteObjectsResult();


        assertEquals(1,  dbSupport.getServiceManager()
                .getRetriever(S3Object.class).getCount(Require.beanPropertyEquals(S3Object.LATEST, true)), "Number of latest objects should be 1");
        assertFalse(mockDaoDriver.attain(S3Object.class, o1v3).isLatest(), "o1v3 should no longer be latest");
        assertFalse(mockDaoDriver.attain(S3Object.class, o2v2).isLatest(), "o2v2 should no longer be latest");
        assertTrue(mockDaoDriver.attain(S3Object.class, o3).isLatest(), "o3 should still be latest");
        final Object expected4 = CollectionFactory.toSet(
         b1.getId(), b2.getId(), b3.getId(), b4.getId(), b5.getId(), b6.getId() );
        assertEquals(expected4, BeanUtils.toMap( 
                 dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet() ).keySet(), "Should notta removed anything.");

        result = service.delete(
                PreviousVersions.UNMARK_LATEST, CollectionFactory.toSet( o1v3.getId(), o2v2.getId(), o3.getId() ) ).toDeleteObjectsResult();

        assertEquals(result.getFailures().length,  2, "Should have had one failure");
        final Object expected3 = result.getFailures()[0].getReason();
        assertEquals(expected3, DeleteObjectFailureReason.NOT_FOUND, "Should have been a 'not found' failure");
        final Object expected2 = result.getFailures()[1].getReason();
        assertEquals(expected2, DeleteObjectFailureReason.NOT_FOUND, "Should have been a 'not found' failure");
        final Object expected1 = result.isDaoModified();
        assertEquals(expected1, true, "Should have considered dao modified");

        assertEquals(0,  dbSupport.getServiceManager()
                .getRetriever(S3Object.class).getCount(Require.beanPropertyEquals(S3Object.LATEST, true)), "Number of latest objects should be 0");
        final Object expected = CollectionFactory.toSet(
         b1.getId(), b2.getId(), b3.getId(), b4.getId(), b5.getId(), b6.getId() );
        assertEquals(expected, BeanUtils.toMap( 
                 dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet() ).keySet(), "Should notta removed anything.");
    }
    
    
    @Test
    public void testDeleteWithListenerResultsInListenerNotifications()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
            
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createABMConfigSingleCopyOnTape();
        final Bucket bucket = mockDaoDriver.createBucket( null, dataPolicy.getId(), MockDaoDriver.DEFAULT_BUCKET_NAME );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        final S3Object o1v1 = mockDaoDriver.createObject( bucket.getId(), "o1", 10, new Date(1000) );
        mockDaoDriver.getBlobFor( o1v1.getId() );
        final S3Object o1v2 = mockDaoDriver.createObject( bucket.getId(), "o1", 10, new Date(2000) );
        mockDaoDriver.getBlobFor( o1v2.getId() );
        final S3Object o1v3 = mockDaoDriver.createObject( bucket.getId(), "o1", 10, new Date(3000) );
        final Blob b3 = mockDaoDriver.getBlobFor( o1v3.getId() );
        final S3Object o2v1 = mockDaoDriver.createObject( bucket.getId(), "o2", 10 );
        final Blob b4 = mockDaoDriver.getBlobFor( o2v1.getId() );
        final S3Object o2v2 = mockDaoDriver.createObject( bucket.getId(), "o2", 10 );
        final Blob b5 = mockDaoDriver.getBlobFor( o2v2.getId() );
        final S3Object o3 = mockDaoDriver.createObject( bucket.getId(), "o3" );
        final Blob b6 = mockDaoDriver.getBlobFor( o3.getId() );
        
        final S3ObjectService service = dbSupport.getServiceManager().getService( S3ObjectService.class );
        service.delete( 
                PreviousVersions.DELETE_ALL_VERSIONS,
                CollectionFactory.toSet( o1v2.getId() ),
                new DeleteS3ObjectsPreCommitListener()
                {
                    public void preparedToCommitDelete( final BeansServiceManager transaction )
                    {
                        transaction.getService( BlobService.class ).delete( b6.getId() );
                    }
                });
        final Object expected = CollectionFactory.toSet(
         b4.getId(), b5.getId() );
        assertEquals(expected, BeanUtils.toMap( 
                 dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll().toSet() ).keySet(), "Shoulda whacked o1v1 and o1v2 and o1v3, plus run the listener which shoulda whacked b6.");
    }
    
    
    //We use "no checksum" to show that a blob has not been uploaded and added to our total yet.
    @Test
    public void testDeletingBlobsWithNoChecksumDoesntImpactLogicalBucketCapacity()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1"); 
            
        final BucketLogicalSizeCache bucketLogicalSizeCache = 
                dbSupport.getServiceManager().getService( BucketService.class ).getLogicalSizeCache();

        assertEquals( 0, bucketLogicalSizeCache.getSize( bucket.getId() ) );

        final S3Object o1 = mockDaoDriver.createObjectStub( bucket.getId(), "o1", 5 );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        bucketLogicalSizeCache.blobCreated( bucket.getId(), b1.getLength() );
        mockDaoDriver.updateBean( b1.setChecksum("ch"), ChecksumObservable.CHECKSUM );
        
        final S3Object o2 = mockDaoDriver.createObjectStub( bucket.getId(), "o2", 10 );
        final Blob b2 = mockDaoDriver.getBlobFor( o2.getId() );
        bucketLogicalSizeCache.blobCreated( bucket.getId(), b2.getLength() );
        mockDaoDriver.updateBean( b2.setChecksum("ch"), ChecksumObservable.CHECKSUM );
        
        final S3Object o3 = mockDaoDriver.createObjectStub( bucket.getId(), "o3", 20 );
        mockDaoDriver.getBlobFor( o3.getId() );

        assertEquals( b1.getLength() + b2.getLength(), bucketLogicalSizeCache.getSize( bucket.getId() ) );

        final S3ObjectService service = dbSupport.getServiceManager().getService( S3ObjectService.class );
        service.delete( 
                PreviousVersions.DELETE_ALL_VERSIONS,
                CollectionFactory.toSet( o1.getId() ) );
        service.delete( 
                PreviousVersions.DELETE_ALL_VERSIONS,
                CollectionFactory.toSet( o3.getId() ) );

        assertEquals( b2.getLength(), bucketLogicalSizeCache.getSize( bucket.getId() ) );
    }
    
    
    @Test
    public void testLogicalBucketCapacityUpdatedCorrectlyWhenDeletingVersionedFiles()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b1");
        final UUID bucketId = bucket.getId();
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( bucketId );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_MULTIPLE_VERSIONS ),
                DataPolicy.VERSIONING ); 
            
        final BucketLogicalSizeCache bucketLogicalSizeCache = 
                dbSupport.getServiceManager().getService( BucketService.class ).getLogicalSizeCache();

        assertEquals( 0, bucketLogicalSizeCache.getSize( bucket.getId() ) );

        final S3Object o1v1 = mockDaoDriver.createObject( bucket.getId(), "o1", 10 );
        final Blob b1v1 = mockDaoDriver.getBlobFor( o1v1.getId() );
        bucketLogicalSizeCache.blobCreated( bucket.getId(), b1v1.getLength() );
        mockDaoDriver.updateBean( b1v1.setChecksum("ch"), ChecksumObservable.CHECKSUM );
        
        final S3Object o1v2 = mockDaoDriver.createObject( bucket.getId(), "o1", 15);
        final Blob b1v2 = mockDaoDriver.getBlobFor( o1v2.getId() );
        bucketLogicalSizeCache.blobCreated( bucket.getId(), b1v2.getLength() );
        mockDaoDriver.updateBean( b1v2.setChecksum("ch"), ChecksumObservable.CHECKSUM );

        assertEquals( b1v1.getLength() + b1v2.getLength(), bucketLogicalSizeCache.getSize( bucket.getId() ) );

        final S3ObjectService service = dbSupport.getServiceManager().getService( S3ObjectService.class );
        service.delete( 
                PreviousVersions.DELETE_SPECIFIC_VERSION,
                CollectionFactory.toSet( o1v2.getId() ) );
        assertEquals( 10, bucketLogicalSizeCache.getSize( bucket.getId() ) );
        service.delete( 
                PreviousVersions.DELETE_SPECIFIC_VERSION,
                CollectionFactory.toSet( o1v1.getId() ) );
        assertEquals( 0, bucketLogicalSizeCache.getSize( bucket.getId() ) );

        final S3Object o2v1 = mockDaoDriver.createObject( bucket.getId(), "o2", 5);
        final Blob b2v1 = mockDaoDriver.getBlobFor( o2v1.getId() );
        bucketLogicalSizeCache.blobCreated( bucket.getId(), b2v1.getLength() );
        mockDaoDriver.updateBean( b2v1.setChecksum("ch"), ChecksumObservable.CHECKSUM );
        
        final S3Object o2v2 = mockDaoDriver.createObject( bucket.getId(), "o2", 10);
        final Blob b2v2 = mockDaoDriver.getBlobFor( o2v2.getId() );
        bucketLogicalSizeCache.blobCreated( bucket.getId(), b2v2.getLength() );
        mockDaoDriver.updateBean( b2v2.setChecksum("ch"), ChecksumObservable.CHECKSUM );
        
        final S3Object o2v3 = mockDaoDriver.createObject( bucket.getId(), "o2", 20);
        final Blob b2v3 = mockDaoDriver.getBlobFor( o2v3.getId() );
        bucketLogicalSizeCache.blobCreated( bucket.getId(), b2v3.getLength() );
        mockDaoDriver.updateBean( b2v3.setChecksum("ch"), ChecksumObservable.CHECKSUM );
        
        service.delete( 
                PreviousVersions.DELETE_SPECIFIC_VERSION,
                CollectionFactory.toSet( o2v3.getId() ) );

        assertEquals( b2v1.getLength() + b2v2.getLength(),
                bucketLogicalSizeCache.getSize( bucket.getId() ) );


    }
}
