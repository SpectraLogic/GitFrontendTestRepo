/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.shared.ChecksumObservable;
import com.spectralogic.s3.common.dao.domain.shared.ImportDirective;
import com.spectralogic.s3.common.dao.domain.shared.ImportPersistenceTargetDirective;
import com.spectralogic.s3.common.dao.domain.shared.KeyValueObservable;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.ImportTapeDirective;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.Ds3Target;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BlobService;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailure;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.rpc.tape.domain.BlobOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.BucketOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectMetadataKeyValue;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectOnMedia;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsOnMedia;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskNoLongerValidException;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.s3.dataplanner.backend.tape.task.TapeTaskUtils.FailureHandling;
import com.spectralogic.s3.dataplanner.backend.tape.task.TapeTaskUtils.RestoreExpected;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class ImportTapeTask_Test 
{
    @Test
    public void testDequeuedWhenImportedSuccessfullyDoesNotChangeTapeState()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule( 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.dequeued();

        assertEquals(TapeState.NORMAL, mockDaoDriver.attain( tape ).getState(), "Should notta changed tape state.");
    }


    @Test
    public void testDequeuedWhenNotImportedSuccessfullyChangesTapeState()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.FOREIGN );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.IMPORT_PENDING );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.dequeued();

        assertEquals(TapeState.FOREIGN, mockDaoDriver.attain( tape ).getState(), "Shoulda changed tape state.");
    }


    @Test
    public void testDequeuedWhenTapeNoLongerExistsDoesNotBlowUp()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape( TapeState.FOREIGN );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                tape, TapeState.IMPORT_PENDING );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        mockDaoDriver.delete( Tape.class, tape );
        task.dequeued();
    }


    @Test
    public void testImportWhenSinglePageOfResultsAndEverythingImportedIsNewWorksNoVerify()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.updateBean(
                directive.setVerifyDataPriorToImport( false ),
                ImportPersistenceTargetDirective.VERIFY_DATA_PRIOR_TO_IMPORT );

        final BasicTestsInvocationHandler blobStoreBtih =
                new BasicTestsInvocationHandler( null );
        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        tapeDriveResource.setVerifyDataResult( BeanFactory.newBean( BlobIoFailures.class ).setFailures(
                new BlobIoFailure [] { BeanFactory.newBean( BlobIoFailure.class ) } ) );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager(), blobStoreBtih );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
        assertEquals(0,  blobStoreBtih.getTotalCallCount(), "Should notta verified tape after import.");
    }


    @Test
    public void testImportWhenSinglePageOfResultsAndEverythingImportedIsNewWorksVerifyPriorToImport()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final BasicTestsInvocationHandler blobStoreBtih =
                new BasicTestsInvocationHandler( null );
        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager(), blobStoreBtih );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
        assertEquals(0,  blobStoreBtih.getTotalCallCount(), "Should notta verified tape after import.");
    }


    @Test
    public void testImportWhenSinglePageOfResults()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.updateBean(
                directive.setVerifyDataPriorToImport( false ),
                ImportPersistenceTargetDirective.VERIFY_DATA_PRIOR_TO_IMPORT );
        mockDaoDriver.updateBean(
                directive.setVerifyDataAfterImport( BlobStoreTaskPriority.LOW ),
                ImportPersistenceTargetDirective.VERIFY_DATA_AFTER_IMPORT );

        final BasicTestsInvocationHandler blobStoreBtih =
                new BasicTestsInvocationHandler( null );
        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        tapeDriveResource.setVerifyDataResult( BeanFactory.newBean( BlobIoFailures.class ).setFailures(
                new BlobIoFailure [] { BeanFactory.newBean( BlobIoFailure.class ) } ) );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager(), blobStoreBtih );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
        assertEquals(1,  blobStoreBtih.getTotalCallCount(), "Shoulda verified tape after import.");
        assertEquals("verify", blobStoreBtih.getMethodInvokeData().get( 0 ).getMethod().getName(), "Shoulda verified tape after import.");
    }


    @Test
    public void testImportWhenMultiplePagesOfResultsAndEverythingImportedIsNewWorks()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( 1 );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
    }


    @Test
    public void testImportLegacyStructureWhereNoTotalBlobCountProvidedWorks()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses =
                constructResponses( Integer.MAX_VALUE, Integer.MAX_VALUE, false, 0, 1, false);
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
    }


    @Test
    public void testImportObjectPartsDoesNotMarkObjectAsCreatedUntilObjectFullyImported()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses =
                constructResponses( Integer.MAX_VALUE, Integer.MAX_VALUE, true, 0, 2, false );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");

        final Set< S3Object > incompleteObjects =
                dbSupport.getServiceManager().getRetriever( S3Object.class ).retrieveAll(
                        S3Object.CREATION_DATE, null ).toSet();
        final Set< UUID > incompleteObjectIds = BeanUtils.toMap( incompleteObjects ).keySet();
        final Set< S3Object > completeObjects =
                dbSupport.getServiceManager().getRetriever( S3Object.class ).retrieveAll(
                        Require.not( Require.beanPropertyEqualsOneOf(
                                Identifiable.ID, incompleteObjectIds ) ) ).toSet();
        assertEquals(8,  incompleteObjects.size(), "Shoulda recorded some objects without a creation date.");
        assertEquals(7,  completeObjects.size(), "Shoulda recorded some objects with a creation date.");

        assertEquals(8,  dbSupport.getServiceManager().getRetriever(S3ObjectProperty.class).getCount(
                KeyValueObservable.KEY, KeyValueObservable.CREATION_DATE), "Shoulda recorded creation dates for objects without a creation date.");
    }


    @Test
    public void testImportZeroLengthObjectsAndFoldersWhenAllContentIsNewWorks()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( 1, 1, true, 0, 1, false );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");

        assertEquals(15,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(), "Shoulda imported every object and folder as zero-length.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(
                Require.beanPropertyEquals(S3Object.CREATION_DATE, null)), "Shoulda imported every object and folder as zero-length.");
        assertEquals(15,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(), "Shoulda imported every object and folder as zero-length.");
        assertEquals(15,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.LENGTH, Long.valueOf(0))), "Shoulda imported every object and folder as zero-length.");
    }


    @Test
    public void testImportZeroLengthObjectsAndFoldersWhenSomeContentAlreadyExistsWorks()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        mockDaoDriver.simulateObjectUploadCompletion( o44.getId() );
        mockDaoDriver.simulateObjectUploadCompletion( o43.getId() );

        final List< S3ObjectsOnMedia > responses =
                constructResponses( Integer.MAX_VALUE, 1, true, 0, 1, false);
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setId( o44.getId() );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");

        assertEquals(15,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(), "Shoulda imported every object and folder as zero-length.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(
                Require.beanPropertyEquals(S3Object.CREATION_DATE, null)), "Shoulda imported every object and folder as zero-length.");
        assertEquals(15,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(), "Shoulda imported every object and folder as zero-length.");
        assertEquals(15,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.LENGTH, Long.valueOf(0))), "Shoulda imported every object and folder as zero-length.");
    }


    @Test
    public void testImportInterleavedBlobsInSingleResponseWorks()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        final List< S3ObjectsOnMedia > responses =
                constructResponses( 1, Integer.MAX_VALUE, true, 0, 1, true);
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");
        assertEquals(15,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(), "Shoulda imported 15 objects.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(
                Require.beanPropertyEquals(S3Object.CREATION_DATE, null)), "Shoulda imported all blobs for each object.");
        assertEquals(20,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(), "Shoulda imported 20 blobs.");
    }


    @Test
    public void testImportWhenOnlyBucketsAlreadyExistWorksWhenNoDataPolicyOrUserIdSpecified()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_1" );
        mockDaoDriver.createBucket( null, "b_2" );
        mockDaoDriver.createObject( bucket.getId(), "someobject" );
        mockDaoDriver.createBucket( null, "b_3" );
        mockDaoDriver.createBucket( null, "b_4" );
        mockDaoDriver.createBucket( null, "b_5" );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.updateBean(
                directive.setDataPolicyId( null ).setUserId( null ),
                ImportDirective.DATA_POLICY_ID, UserIdObservable.USER_ID );

        final List< S3ObjectsOnMedia > responses = constructResponses( 1 );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
    }


    @Test
    public void testImportWhenOnlyBucketsAlreadyExistWorksWhenDifferentDataPoliciesBetweenBuckets()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy(
                "dp2" );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_1" );
        mockDaoDriver.createBucket( null, dataPolicy2.getId(), "b_2" );
        mockDaoDriver.createObject( bucket.getId(), "someobject" );
        mockDaoDriver.createBucket( null, "b_3" );
        mockDaoDriver.createBucket( null, dataPolicy2.getId(), "b_4" );
        mockDaoDriver.createBucket( null, "b_5" );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.updateBean(
                directive.setDataPolicyId( null ).setUserId( null ),
                ImportDirective.DATA_POLICY_ID, UserIdObservable.USER_ID );

        final List< S3ObjectsOnMedia > responses = constructResponses( 1 );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
    }


    @Test
    public void testDegradedBlobRecordsUpdatedCorrectlyWhenAllDataIngressedIsNew()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy(
                "dp2" );
        final DataPolicy dataPolicy3 = mockDaoDriver.createDataPolicy(
                "dp3" );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain storageDomain2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain storageDomain3 =
                mockDaoDriver.createStorageDomain( "sd3" );
        final StorageDomain storageDomain4 =
                mockDaoDriver.createStorageDomain( "sd4" );
        final AzureTarget azureTarget = mockDaoDriver.createAzureTarget("azureTarget");
        final S3Target s3target = mockDaoDriver.createS3Target("s3Target");
        final Ds3Target ds3target = mockDaoDriver.createDs3Target("ds3Target");
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final DataPersistenceRule rule22 = mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.PERMANENT, storageDomain2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy3.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final DataPersistenceRule rule32 = mockDaoDriver.createDataPersistenceRule(
                dataPolicy3.getId(), DataPersistenceRuleType.PERMANENT, storageDomain2.getId() );
        final DataPersistenceRule rule33 = mockDaoDriver.createDataPersistenceRule(
                dataPolicy3.getId(), DataPersistenceRuleType.PERMANENT, storageDomain3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy3.getId(), DataPersistenceRuleType.RETIRED, storageDomain4.getId() );
        final AzureDataReplicationRule azureRule = mockDaoDriver.createAzureDataReplicationRule(dataPolicy3.getId(), DataReplicationRuleType.PERMANENT, azureTarget.getId());
        final S3DataReplicationRule s3Rule = mockDaoDriver.createS3DataReplicationRule(dataPolicy3.getId(), DataReplicationRuleType.PERMANENT, s3target.getId());
        final Ds3DataReplicationRule ds3Rule = mockDaoDriver.createDs3DataReplicationRule(dataPolicy3.getId(), DataReplicationRuleType.PERMANENT, ds3target.getId());
        mockDaoDriver.createBucket( null, dataPolicy.getId(), "b_1" );
        mockDaoDriver.createBucket( null, dataPolicy2.getId(), "b_2" );
        mockDaoDriver.createBucket( null, dataPolicy3.getId(), "b_3" );
        mockDaoDriver.createBucket( null, dataPolicy.getId(), "b_4" );
        mockDaoDriver.createBucket( null, dataPolicy2.getId(), "b_5" );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.updateBean(
                directive.setDataPolicyId( null ).setUserId( null ),
                ImportDirective.DATA_POLICY_ID, UserIdObservable.USER_ID );

        final List< S3ObjectsOnMedia > responses = constructResponses( 1 );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");


        final BeansRetriever< S3Object > objectRetriever =
                dbSupport.getServiceManager().getRetriever( S3Object.class );
        final UUID o11 = objectRetriever.attain( NameObservable.NAME, "o_1blobs_1present" ).getId();
        final UUID o21 = objectRetriever.attain( NameObservable.NAME, "o_2blobs_1present" ).getId();
        final UUID o22 = objectRetriever.attain( NameObservable.NAME, "o_2blobs_2present" ).getId();
        final UUID o31 = objectRetriever.attain( NameObservable.NAME, "o_3blobs_1present" ).getId();
        final UUID o32 = objectRetriever.attain( NameObservable.NAME, "o_3blobs_2present" ).getId();
        final UUID o33 = objectRetriever.attain( NameObservable.NAME, "o_3blobs_3present" ).getId();
        final UUID o41 = objectRetriever.attain( NameObservable.NAME, "o_4blobs_1present" ).getId();
        final UUID o42 = objectRetriever.attain( NameObservable.NAME, "o_4blobs_2present" ).getId();
        final UUID o43 = objectRetriever.attain( NameObservable.NAME, "o_4blobs_3present" ).getId();
        final UUID o44 = objectRetriever.attain( NameObservable.NAME, "o_4blobs_4present" ).getId();

        verifyDegradedBlobs( dbSupport, o11, rule22.getId() );
        verifyDegradedBlobs( dbSupport, o21, rule32.getId(), rule33.getId() );
        verifyDegradedBlobs( dbSupport, o22, rule32.getId(), rule33.getId() );
        verifyDegradedBlobs( dbSupport, o21, DegradedBlob.AZURE_REPLICATION_RULE_ID, azureRule.getId() );
        verifyDegradedBlobs( dbSupport, o22, DegradedBlob.AZURE_REPLICATION_RULE_ID, azureRule.getId() );
        verifyDegradedBlobs( dbSupport, o21, DegradedBlob.S3_REPLICATION_RULE_ID, s3Rule.getId() );
        verifyDegradedBlobs( dbSupport, o22, DegradedBlob.S3_REPLICATION_RULE_ID, s3Rule.getId() );
        verifyDegradedBlobs( dbSupport, o21, DegradedBlob.DS3_REPLICATION_RULE_ID, ds3Rule.getId() );
        verifyDegradedBlobs( dbSupport, o22, DegradedBlob.DS3_REPLICATION_RULE_ID, ds3Rule.getId() );
        verifyDegradedBlobs( dbSupport, o31 );
        verifyDegradedBlobs( dbSupport, o32 );
        verifyDegradedBlobs( dbSupport, o33 );
        verifyDegradedBlobs( dbSupport, o41, rule22.getId() );
        verifyDegradedBlobs( dbSupport, o42, rule22.getId() );
        verifyDegradedBlobs( dbSupport, o43, rule22.getId() );
        verifyDegradedBlobs( dbSupport, o44, rule22.getId() );
    }


    @Test
    public void testDegradedBlobRecordsUpdatedCorrectlyWhenNotAllDataIngressedIsNew()
    {
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy(
                "dp2" );
        final DataPolicy dataPolicy3 = mockDaoDriver.createDataPolicy(
                "dp3" );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain storageDomain2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain storageDomain3 =
                mockDaoDriver.createStorageDomain( "sd3" );
        final StorageDomain storageDomain4 =
                mockDaoDriver.createStorageDomain( "sd4" );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final DataPersistenceRule rule21 = mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final DataPersistenceRule rule22 = mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.PERMANENT, storageDomain2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain3.getId() );
        final DataPersistenceRule rule31 = mockDaoDriver.createDataPersistenceRule(
                dataPolicy3.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final DataPersistenceRule rule32 = mockDaoDriver.createDataPersistenceRule(
                dataPolicy3.getId(), DataPersistenceRuleType.PERMANENT, storageDomain2.getId() );
        final DataPersistenceRule rule33 = mockDaoDriver.createDataPersistenceRule(
                dataPolicy3.getId(), DataPersistenceRuleType.PERMANENT, storageDomain3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy3.getId(), DataPersistenceRuleType.RETIRED, storageDomain4.getId() );
        mockDaoDriver.createBucket( null, dataPolicy.getId(), "b_1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, dataPolicy2.getId(), "b_2" );
        final Bucket bucket3 = mockDaoDriver.createBucket( null, dataPolicy3.getId(), "b_3" );
        mockDaoDriver.createBucket( null, dataPolicy.getId(), "b_4" );
        mockDaoDriver.createBucket( null, dataPolicy2.getId(), "b_5" );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.updateBean(
                directive.setDataPolicyId( null ).setUserId( null ),
                ImportDirective.DATA_POLICY_ID, UserIdObservable.USER_ID );

        final BeansRetriever< S3Object > objectRetriever =
                dbSupport.getServiceManager().getRetriever( S3Object.class );
        final S3Object o11 = mockDaoDriver.createObject( bucket2.getId(), "o_1blobs_1present", -1 );
        final Blob b11 = mockDaoDriver.createBlobs( o11.getId(), 1, 1000 ).get( 0 );
        mockDaoDriver.simulateObjectUploadCompletion( o11.getId() );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( DegradedBlob.class )
                .setBlobId( b11.getId() )
                .setBucketId( bucket2.getId() )
                .setPersistenceRuleId( rule21.getId() ) );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final S3Object o22 = mockDaoDriver.createObject( bucket3.getId(), "o_2blobs_2present", -1 );
        final List< Blob > blobs2 = mockDaoDriver.createBlobs( o22.getId(), 2, 1000 );
        final Blob b21 = blobs2.get( 0 );
        final Blob b22 = blobs2.get( 1 );
        mockDaoDriver.simulateObjectUploadCompletion( o22.getId() );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( DegradedBlob.class )
                .setBlobId( b21.getId() )
                .setBucketId( bucket3.getId() )
                .setPersistenceRuleId( rule31.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( DegradedBlob.class )
                .setBlobId( b21.getId() )
                .setBucketId( bucket3.getId() )
                .setPersistenceRuleId( rule32.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( DegradedBlob.class )
                .setBlobId( b22.getId() )
                .setBucketId( bucket3.getId() )
                .setPersistenceRuleId( rule31.getId() ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( DegradedBlob.class )
                .setBlobId( b22.getId() )
                .setBucketId( bucket3.getId() )
                .setPersistenceRuleId( rule33.getId() ) );
        responses.get( 0 ).getBuckets()[ 1 ].getObjects()[ 1 ].setId( o11.getId() );
        responses.get( 0 ).getBuckets()[ 1 ].getObjects()[ 1 ].getBlobs()[ 0 ].setId( b11.getId() );
        responses.get( 0 ).getBuckets()[ 1 ].getObjects()[ 1 ].getBlobs()[ 0 ].setChecksum(
                dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                        b11.getId() ).getChecksum() );

        responses.get( 0 ).getBuckets()[ 2 ].getObjects()[ 2 ].setId( o22.getId() );
        responses.get( 0 ).getBuckets()[ 2 ].getObjects()[ 2 ].getBlobs()[ 0 ].setId( b21.getId() );
        responses.get( 0 ).getBuckets()[ 2 ].getObjects()[ 2 ].getBlobs()[ 0 ].setChecksum(
                dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                        b21.getId() ).getChecksum() );
        responses.get( 0 ).getBuckets()[ 2 ].getObjects()[ 2 ].getBlobs()[ 1 ].setId( b22.getId() );
        responses.get( 0 ).getBuckets()[ 2 ].getObjects()[ 2 ].getBlobs()[ 1 ].setChecksum(
                dbSupport.getServiceManager().getRetriever( Blob.class ).attain(
                        b22.getId() ).getChecksum() );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");

        final UUID o21 = objectRetriever.attain( NameObservable.NAME, "o_2blobs_1present" ).getId();
        final UUID o31 = objectRetriever.attain( NameObservable.NAME, "o_3blobs_1present" ).getId();
        final UUID o32 = objectRetriever.attain( NameObservable.NAME, "o_3blobs_2present" ).getId();
        final UUID o33 = objectRetriever.attain( NameObservable.NAME, "o_3blobs_3present" ).getId();
        final UUID o41 = objectRetriever.attain( NameObservable.NAME, "o_4blobs_1present" ).getId();
        final UUID o42 = objectRetriever.attain( NameObservable.NAME, "o_4blobs_2present" ).getId();
        final UUID o43 = objectRetriever.attain( NameObservable.NAME, "o_4blobs_3present" ).getId();
        final UUID o44 = objectRetriever.attain( NameObservable.NAME, "o_4blobs_4present" ).getId();

        verifyDegradedBlobs( dbSupport, o11.getId() );
        verifyDegradedBlobs( dbSupport, o21, rule32.getId(), rule33.getId() );
        verifyDegradedBlobs( dbSupport, CollectionFactory.toSet( b21.getId() ), rule32.getId() );
        verifyDegradedBlobs( dbSupport, CollectionFactory.toSet( b22.getId() ), rule33.getId() );
        verifyDegradedBlobs( dbSupport, o31 );
        verifyDegradedBlobs( dbSupport, o32 );
        verifyDegradedBlobs( dbSupport, o33 );
        verifyDegradedBlobs( dbSupport, o41, rule22.getId() );
        verifyDegradedBlobs( dbSupport, o42, rule22.getId() );
        verifyDegradedBlobs( dbSupport, o43, rule22.getId() );
        verifyDegradedBlobs( dbSupport, o44, rule22.getId() );
    }


    private void verifyDegradedBlobs(
            final DatabaseSupport dbSupport,
            final UUID objectId,
            final UUID ... persistenceRuleIds )
    {
        verifyDegradedBlobs(
                dbSupport,
                BeanUtils.toMap( dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll(
                        Blob.OBJECT_ID, objectId ).toSet() ).keySet(), persistenceRuleIds );
    }


    private void verifyDegradedBlobs(
            final DatabaseSupport dbSupport,
            final UUID objectId,
            final String degradedBlobProperty,
            final UUID ... persistenceRuleIds )
    {
        verifyDegradedBlobs(
                dbSupport,
                BeanUtils.toMap( dbSupport.getServiceManager().getRetriever( Blob.class ).retrieveAll(
                        Blob.OBJECT_ID, objectId ).toSet() ).keySet(), degradedBlobProperty, persistenceRuleIds );
    }


    private void verifyDegradedBlobs(
            final DatabaseSupport dbSupport,
            final Set< UUID > blobIds,
            final UUID ... persistenceRuleIds )
    {
        verifyDegradedBlobs(
                dbSupport,
                blobIds,
                DegradedBlob.PERSISTENCE_RULE_ID,
                persistenceRuleIds );
    }


    private void verifyDegradedBlobs(
            final DatabaseSupport dbSupport,
            final Set< UUID > blobIds,
            final String degradedBlobProperty,
            final UUID ... ruleIds )
    {
        for ( final UUID blobId : blobIds )
        {
            final Set< DegradedBlob > degradedBlobs =
                    dbSupport.getServiceManager().getRetriever( DegradedBlob.class ).retrieveAll(
                            Require.all(
                                    Require.beanPropertyEquals(BlobObservable.BLOB_ID, blobId),
                                    Require.beanPropertyNotNull(degradedBlobProperty)
                            ) ).toSet();
            final Set< UUID > degradedPersistenceRules =
                    BeanUtils.extractPropertyValues( degradedBlobs, degradedBlobProperty );
            final Object expected = CollectionFactory.toSet( ruleIds );
            assertEquals(expected, degradedPersistenceRules, "Shoulda had degraded blobs as expected for blob " + blobId + ".");
        }
    }


    @Test
    public void testImportWhenOnlyBucketsAlreadyExistWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_1" );
        mockDaoDriver.createObject( bucket.getId(), "someobject" );
        mockDaoDriver.createBucket( null, "b_3" );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( 1 );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
    }


    @Test
    public void testImportWhenNonOverlappingObjectPartsAlreadyExistWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createBucket( null, "b_2" );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        mockDaoDriver.createBlobs( o44.getId(), 3, 4000, 10 );
        mockDaoDriver.createBlobs( o43.getId(), 3, 4000, 10 );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setId( o44.getId() );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
        assertEquals(6,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o43.getId())), "Shoulda imported new blobs without negating existing blobs.");
        assertEquals(7,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o44.getId())), "Shoulda imported new blobs without negating existing blobs.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");
    }


    @Test
    public void testImportWhenOverlappingNonConflictingObjectPartsAlreadyExistWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o44.getId(), 3, 2000, 1000 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o43.getId(), 3, 2000, 1000 );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "143" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "144" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "149" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "150" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setId( o44.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 1 ].setId(
                blobs3.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 2 ].setId(
                blobs3.get( 1 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 2 ].setId(
                blobs4.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 3 ].setId(
                blobs4.get( 1 ).getId() );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o43.getId())), "Shoulda imported new blobs without negating existing blobs.");
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o44.getId())), "Shoulda imported new blobs without negating existing blobs.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");
    }


    @Test
    public void testImportWhenCreationDateMissingSucceedsButWarnsIncomplete()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object o44 = mockDaoDriver.createObjectStub( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObjectStub( bucket.getId(), "o_4blobs_3present", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o44.getId(), 4, 0, 1000 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o43.getId(), 3, 2000, 1000 );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "143" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "144" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "147" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "148" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 2 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "149" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 3 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "150" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setId( o44.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 1 ].setId(
                blobs3.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 2 ].setId(
                blobs3.get( 1 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 0 ].setId(
                blobs4.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 1 ].setId(
                blobs4.get( 1 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 2 ].setId(
                blobs4.get( 2 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 3 ].setId(
                blobs4.get( 3 ).getId() );

        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setMetadata( removeCreationDate(
                responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getMetadata() ) );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setMetadata( removeCreationDate(
                responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getMetadata() ) );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", new ArrayList<>( responses ) );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");

        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");

        assertEquals(TapeFailureType.IMPORT_INCOMPLETE, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain(
                        Require.nothing() ).getType(), "Shoulda been a warning for incomplete object.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
    }


    @Test
    public void testImportWhenOverlappingNonConflictingObjectPartsAlreadyExistWhenCreationDateNotOnTpWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o44.getId(), 4, 0, 1000 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o43.getId(), 3, 2000, 1000 );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "143" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "144" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "147" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "148" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 2 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "149" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 3 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "150" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setId( o44.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 1 ].setId(
                blobs3.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 2 ].setId(
                blobs3.get( 1 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 0 ].setId(
                blobs4.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 1 ].setId(
                blobs4.get( 1 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 2 ].setId(
                blobs4.get( 2 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 3 ].setId(
                blobs4.get( 3 ).getId() );

        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setMetadata( removeCreationDate(
                responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getMetadata() ) );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setMetadata( removeCreationDate(
                responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getMetadata() ) );

        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", new ArrayList<>( responses ) );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        mockDaoDriver.updateBean( tape.setSerialNumber( null ), SerialNumberObservable.SERIAL_NUMBER );
        dbSupport.getServiceManager().getService( TapeService.class ).transistState(
                mockDaoDriver.attain( tape ), TapeState.IMPORT_PENDING );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.updateBean( o43.setCreationDate( new Date() ), S3Object.CREATION_DATE );
        mockDaoDriver.updateBean( o44.setCreationDate( new Date() ), S3Object.CREATION_DATE );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");
    }


    private S3ObjectMetadataKeyValue [] removeCreationDate( final S3ObjectMetadataKeyValue [] metadata )
    {
        final List< S3ObjectMetadataKeyValue > retval = CollectionFactory.toList( metadata );
        for ( final S3ObjectMetadataKeyValue kv : new HashSet<>( retval ) )
        {
            if ( kv.getKey().equals( KeyValueObservable.CREATION_DATE ) )
            {
                retval.remove( kv );
            }
        }

        return CollectionFactory.toArray( S3ObjectMetadataKeyValue.class, retval );
    }


    @Test
    public void testImportWhenOverlappingNonConflictingObjectPartsAlreadyExistAndRecordedOnTapeWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object otherObject = mockDaoDriver.createObject( bucket.getId(), "otherobject" );
        final Blob otherBlob = mockDaoDriver.getBlobFor( otherObject.getId() );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o44.getId(), 3, 2000, 1000 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o43.getId(), 3, 2000, 1000 );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "143" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "144" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "149" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "150" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        mockDaoDriver.putBlobOnTape( tape.getId(), blobs3.get( 0 ).getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blobs3.get( 1 ).getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blobs4.get( 0 ).getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), otherBlob.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setId( o44.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 1 ].setId(
                blobs3.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 2 ].setId(
                blobs3.get( 1 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 2 ].setId(
                blobs4.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 3 ].setId(
                blobs4.get( 1 ).getId() );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( tape.getId() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(
                BlobObservable.BLOB_ID, otherBlob.getId()), "Shoulda recorded blob suspect since it no longer resides on tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(
                BlobObservable.BLOB_ID, otherBlob.getId()), "Should notta whacked blob tape record for blob that no longer resides on tape.");
        mockDaoDriver.delete( Blob.class, otherBlob );
        mockDaoDriver.delete( S3Object.class, otherObject );

        verifyEverythingHasBeenImported( dbSupport.getServiceManager(), 20, false );
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o43.getId())), "Shoulda imported new blobs without negating existing blobs.");
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o44.getId())), "Shoulda imported new blobs without negating existing blobs.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");
        assertFalse(mockDaoDriver.attain( tape ).isTakeOwnershipPending(), "Should notta recorded take ownership is pending.");
        assertNotNull(dbSupport.getServiceManager().getRetriever( S3ObjectProperty.class ).attain( Require.all(
                        Require.beanPropertyEquals( S3ObjectProperty.OBJECT_ID, o43.getId() ),
                        Require.beanPropertyEquals(
                                KeyValueObservable.KEY,
                                S3HeaderType.ETAG.getHttpHeaderName() ) ) ), "Shoulda imported etag.");
    }


    @Test
    public void testImportWhenOverlappingNonConflictingObjectPartsAlreadyExistAndRecordedOnReadOnlyTpWorks()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object otherObject = mockDaoDriver.createObject( bucket.getId(), "otherobject" );
        final Blob otherBlob = mockDaoDriver.getBlobFor( otherObject.getId() );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o44.getId(), 3, 2000, 1000 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o43.getId(), 3, 2000, 1000 );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "143" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "144" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "149" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "150" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        mockDaoDriver.putBlobOnTape( tape.getId(), blobs3.get( 0 ).getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blobs3.get( 1 ).getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blobs4.get( 0 ).getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), otherBlob.getId() );

        /*
         * Import tape
         */
        List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setId( o44.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 1 ].setId(
                blobs3.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 2 ].setId(
                blobs3.get( 1 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 2 ].setId(
                blobs4.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 3 ].setId(
                blobs4.get( 1 ).getId() );
        MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setLoadedTapeReadOnly( true );
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( tape.getId() );
        ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected1 = tape.getId();
        assertEquals(expected1, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(SuspectBlobTape.class).getCount(
                BlobObservable.BLOB_ID, otherBlob.getId()), "Shoulda recorded blob suspect since it no longer resides on tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(
                BlobObservable.BLOB_ID, otherBlob.getId()), "Should notta whacked blob tape record for blob that no longer resides on tape.");
        mockDaoDriver.delete( Blob.class, otherBlob );
        mockDaoDriver.delete( S3Object.class, otherObject );

        verifyEverythingHasBeenImported( dbSupport.getServiceManager(), 20, false );
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o43.getId())), "Shoulda imported new blobs without negating existing blobs.");
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o44.getId())), "Shoulda imported new blobs without negating existing blobs.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any failures.");
        assertTrue(mockDaoDriver.attain( tape ).isTakeOwnershipPending(), "Shoulda recorded take ownership is pending since tape was write-protected.");

        /*
         * Put tape in state where re-import necessary
         */
        final MockTapeDriveResource failingTapeDriveResource = new MockTapeDriveResource();
        mockDaoDriver.updateBean( tape.setLastCheckpoint( "cc" ), Tape.LAST_CHECKPOINT );
        failingTapeDriveResource.setVerifyQuiescedToCheckpointException(
                new RpcProxyException( "Oops", BeanFactory.newBean( Failure.class ) ) );
        TapeTaskUtils.verifyQuiescedToCheckpoint(
                mockDaoDriver.attain( tape ),
                failingTapeDriveResource,
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.NO,
                FailureHandling.LOG_IT );
        mockDaoDriver.attainAndUpdate( tape );
        assertEquals(TapeState.FOREIGN, tape.getState(), "Shoulda updated tape's state and noted re-import necessary.");
        assertFalse(tape.isTakeOwnershipPending(), "Shoulda updated tape's state and noted re-import necessary.");
        assertNotNull(tape.getStorageDomainMemberId(), "Shoulda updated tape's state and noted re-import necessary.");
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        dbSupport.getDataManager().deleteBeans(
                Blob.class,
                Require.not( Require.exists(
                        Blob.OBJECT_ID,
                        Require.beanPropertyEqualsOneOf(
                                Identifiable.ID,
                                CollectionFactory.toSet( o43.getId(), o44.getId() ) ) ) ) );
        dbSupport.getDataManager().deleteBeans(
                S3Object.class,
                Require.not( Require.beanPropertyEqualsOneOf(
                        Identifiable.ID,
                        CollectionFactory.toSet( o43.getId(), o44.getId() ) ) ) );

        /*
         * Re-import tape
         */
        final BeansRetriever< Blob > blobRetriever =
                dbSupport.getServiceManager().getRetriever( Blob.class );
        directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setId( o44.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 0 ].setId(
                blobRetriever.attain( Require.all(
                        Require.beanPropertyEquals( Blob.OBJECT_ID, o43.getId() ),
                        Require.beanPropertyEquals( Blob.BYTE_OFFSET, Long.valueOf( 1000 ) ) ) ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 1 ].setId(
                blobRetriever.attain( Require.all(
                        Require.beanPropertyEquals( Blob.OBJECT_ID, o43.getId() ),
                        Require.beanPropertyEquals( Blob.BYTE_OFFSET, Long.valueOf( 2000 ) ) ) ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 2 ].setId(
                blobRetriever.attain( Require.all(
                        Require.beanPropertyEquals( Blob.OBJECT_ID, o43.getId() ),
                        Require.beanPropertyEquals( Blob.BYTE_OFFSET, Long.valueOf( 3000 ) ) ) ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 0 ].setId(
                blobRetriever.attain( Require.all(
                        Require.beanPropertyEquals( Blob.OBJECT_ID, o44.getId() ),
                        Require.beanPropertyEquals( Blob.BYTE_OFFSET, Long.valueOf( 0 ) ) ) ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 1 ].setId(
                blobRetriever.attain( Require.all(
                        Require.beanPropertyEquals( Blob.OBJECT_ID, o44.getId() ),
                        Require.beanPropertyEquals( Blob.BYTE_OFFSET, Long.valueOf( 1000 ) ) ) ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 2 ].setId(
                blobRetriever.attain( Require.all(
                        Require.beanPropertyEquals( Blob.OBJECT_ID, o44.getId() ),
                        Require.beanPropertyEquals( Blob.BYTE_OFFSET, Long.valueOf( 2000 ) ) ) ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 3 ].setId(
                blobRetriever.attain( Require.all(
                        Require.beanPropertyEquals( Blob.OBJECT_ID, o44.getId() ),
                        Require.beanPropertyEquals( Blob.BYTE_OFFSET, Long.valueOf( 3000 ) ) ) ).getId() );
        tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setTapeSerialNumber( tape.getSerialNumber() );
        tapeDriveResource.setLoadedTapeReadOnly( true );
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( tape.getId() );
        task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        final StorageDomain otherStorageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.updateBean(
                directive.setStorageDomainId( otherStorageDomain.getId() ),
                ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");

        mockDaoDriver.updateBean(
                directive.setStorageDomainId( null ),
                ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID );
        dbSupport.getDataManager().createBean( directive );
        mockDaoDriver.updateBean( tape.setState( TapeState.IMPORT_PENDING ), Tape.STATE );
        task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(BlobTape.class).getCount(
                BlobObservable.BLOB_ID, otherBlob.getId()), "Shoulda whacked blob tape record for blob that no longer resides on tape.");
        mockDaoDriver.delete( Blob.class, otherBlob );
        mockDaoDriver.delete( S3Object.class, otherObject );

        verifyEverythingHasBeenImported( dbSupport.getServiceManager(), 20, false );
        assertEquals(4,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o43.getId())), "Shoulda imported new blobs without negating existing blobs.");
        assertEquals(5,  dbSupport.getServiceManager().getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o44.getId())), "Shoulda imported new blobs without negating existing blobs.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda cleared all import failed failures due to successful import.");
        assertTrue(mockDaoDriver.attain( tape ).isTakeOwnershipPending(), "Shoulda recorded take ownership is pending since tape was write-protected.");
    }


    public void
    testImportWhenOverlappingNonConflictingObjectPartsAlreadyExistAndRecordedOnInconsistentReadOnlyTpFails()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object otherObject = mockDaoDriver.createObject( bucket.getId(), "otherobject" );
        final Blob otherBlob = mockDaoDriver.getBlobFor( otherObject.getId() );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o44.getId(), 3, 2000, 1000 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o43.getId(), 3, 2000, 1000 );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "143" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "144" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "149" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "150" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        mockDaoDriver.putBlobOnTape( tape.getId(), blobs3.get( 0 ).getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blobs3.get( 1 ).getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), blobs4.get( 0 ).getId() );
        mockDaoDriver.putBlobOnTape( tape.getId(), otherBlob.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setId( o44.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 1 ].setId(
                blobs3.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 2 ].setId(
                blobs3.get( 1 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 2 ].setId(
                blobs4.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 3 ].setId(
                blobs4.get( 1 ).getId() );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setLoadedTapeReadOnly( true );
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( tape.getId() );
        tapeDriveResource.setVerifyQuiescedToCheckpointException( new RuntimeException( "Oops." ) );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been failure.");
        assertFalse(mockDaoDriver.attain( tape ).isTakeOwnershipPending(), "Should notta recorded take ownership is pending since tape not imported.");
    }


    @Test
    public void testImportWhenDifferentBucketsNeedToBePlacedIntoDifferentStorageDomainsNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy(
                "dp2" );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain storageDomain2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain2.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.PERMANENT, storageDomain2.getId() );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_1" );
        mockDaoDriver.createBucket( null, dataPolicy2.getId(), "b_2" );
        mockDaoDriver.createObject( bucket.getId(), "someobject" );
        mockDaoDriver.createBucket( null, "b_3" );
        mockDaoDriver.createBucket( null, dataPolicy2.getId(), "b_4" );
        mockDaoDriver.createBucket( null, "b_5" );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.updateBean(
                directive.setDataPolicyId( null ).setUserId( null ),
                ImportDirective.DATA_POLICY_ID, UserIdObservable.USER_ID );

        final List< S3ObjectsOnMedia > responses = constructResponses( 1 );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    @Test
    public void testImportEverythingImportedIsNewButNoDataPolicySpecifiedNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.updateBean( directive.setDataPolicyId( null ), ImportDirective.DATA_POLICY_ID );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    @Test
    public void testImportEverythingImportedIsNewButNoUserIdSpecifiedNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.updateBean( directive.setUserId( null ), UserIdObservable.USER_ID );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    @Test
    public void testImportWhenNoCandidateStorageDomainToImportToNotAllowed1()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO6 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    @Test
    public void testImportWhenNoCandidateStorageDomainToImportToNotAllowed2()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    @Test
    public void testImportWhenNoCandidateStorageDomainToImportToNotAllowed3()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    public void
testImportWhenMultiplePotentialStorageDomainsToImportIntoWhenExplicitStorageDomainIdSpecInDirectiveAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain storageDomain2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain storageDomain3 =
                mockDaoDriver.createStorageDomain( "sd3" );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final StorageDomainMember sdm1 = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain2.getId(), tapePartition.getId(), TapeType.LTO5 );
        final StorageDomainMember sdm3 = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain3.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain3.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.updateBean(
                directive.setStorageDomainId( storageDomain.getId() ),
                ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = sdm1.getId();
        assertEquals(expected, tapeService.attain( tape.getId() ).getStorageDomainMemberId(), "Shoulda successfully imported tape.");
        assertTrue(tapeService.attain( tape.getId() ).isAssignedToStorageDomain(), "Shoulda successfully imported tape.");
        assertEquals(null, tapeService.attain( tape.getId() ).getBucketId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been a failure.");
    }


    public void
    testImportWhenExplicitlySpecifiedStorageDomainViaImportDirectiveNotValidOptionNotAllowedSDNotTargeted()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain storageDomain2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain storageDomain3 =
                mockDaoDriver.createStorageDomain( "sd3" );
        final StorageDomain storageDomain4 =
                mockDaoDriver.createStorageDomain( "sd4" );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain2.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain3.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain3.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.updateBean(
                directive.setStorageDomainId( storageDomain4.getId() ),
                ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(null, tapeService.attain( tape.getId() ).getStorageDomainMemberId(), "Should notta imported tape.");
        assertFalse(tapeService.attain( tape.getId() ).isAssignedToStorageDomain(), "Should notta imported tape.");
        assertEquals(null, tapeService.attain( tape.getId() ).getBucketId(), "Should notta imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    public void
    testImportWhenExplicitlySpecifiedStorageDomainViaImportDirectiveNotValidOptionNotAllowedSDIsTargeted()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain storageDomain2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain storageDomain3 =
                mockDaoDriver.createStorageDomain( "sd3" );
        final StorageDomain storageDomain4 =
                mockDaoDriver.createStorageDomain( "sd4" );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain2.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain3.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain3.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain4.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );
        mockDaoDriver.updateBean(
                directive.setStorageDomainId( storageDomain4.getId() ),
                ImportPersistenceTargetDirective.STORAGE_DOMAIN_ID );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(null, tapeService.attain( tape.getId() ).getStorageDomainMemberId(), "Should notta imported tape.");
        assertFalse(tapeService.attain( tape.getId() ).isAssignedToStorageDomain(), "Should notta imported tape.");
        assertEquals(null, tapeService.attain( tape.getId() ).getBucketId(), "Should notta imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    public void
testImportWhenMultiplePotentialStorageDomainsToImportIntoSuchThatPersistnceRuleTypeDoesNotBreakTieNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain storageDomain2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain storageDomain3 =
                mockDaoDriver.createStorageDomain( "sd3" );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain2.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain3.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain3.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(null, tapeService.attain( tape.getId() ).getStorageDomainMemberId(), "Should notta imported tape.");
        assertFalse(tapeService.attain( tape.getId() ).isAssignedToStorageDomain(), "Should notta imported tape.");
        assertEquals(null, tapeService.attain( tape.getId() ).getBucketId(), "Should notta imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    public void
    testImportWhenOneBucketOutOfManyCannotHaveStorageDomainAssignmentDeterminedNotAllowedWhenBucketNew()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain storageDomain2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain storageDomain3 =
                mockDaoDriver.createStorageDomain( "sd3" );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain2.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain3.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain3.getId() );

        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy(
                "datapolicy2" );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.PERMANENT, storageDomain3.getId() );
        mockDaoDriver.createBucket( null, dataPolicy2.getId(), "b_3" );

        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(null, tapeService.attain( tape.getId() ).getStorageDomainMemberId(), "Should notta imported tape.");
        assertFalse(tapeService.attain( tape.getId() ).isAssignedToStorageDomain(), "Should notta imported tape.");
        assertEquals(null, tapeService.attain( tape.getId() ).getBucketId(), "Should notta imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    public void
    testImportWhenOneBucketOutOfManyCannotHaveStorageDomainAssignmentDeterminedNotAllowedWhenBucketExists()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain storageDomain2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain storageDomain3 =
                mockDaoDriver.createStorageDomain( "sd3" );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain2.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain3.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain3.getId() );

        final DataPolicy dataPolicy2 = mockDaoDriver.createDataPolicy(
                "datapolicy2" );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy2.getId(), DataPersistenceRuleType.PERMANENT, storageDomain3.getId() );
        mockDaoDriver.createBucket( null, dataPolicy.getId(), "b_1" );
        mockDaoDriver.createBucket( null, dataPolicy2.getId(), "b_3" );

        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy2.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(null, tapeService.attain( tape.getId() ).getStorageDomainMemberId(), "Should notta imported tape.");
        assertFalse(tapeService.attain( tape.getId() ).isAssignedToStorageDomain(), "Should notta imported tape.");
        assertEquals(null, tapeService.attain( tape.getId() ).getBucketId(), "Should notta imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    public void
    testImportWhenMultiplePotentialStorageDomainsToImportIntoSuchThatPersistenceRuleTypeDoesNotBreakTie()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final StorageDomain storageDomain2 =
                mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomain storageDomain3 =
                mockDaoDriver.createStorageDomain( "sd3" );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain2.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain3.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain2.getId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.TEMPORARY, storageDomain3.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(null, tapeService.attain( tape.getId() ).getStorageDomainMemberId(), "Should notta imported tape.");
        assertFalse(tapeService.attain( tape.getId() ).isAssignedToStorageDomain(), "Should notta imported tape.");
        assertEquals(null, tapeService.attain( tape.getId() ).getBucketId(), "Should notta imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    @Test
    public void testImportWhenSingleBucketOnTapeAndDataPolicyHasBucketIsolationAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        final StorageDomainMember sdm = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED,
                dataPolicy.getId(),
                DataPersistenceRuleType.PERMANENT,
                storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).setBuckets( new BucketOnMedia [] { responses.get( 0 ).getBuckets()[ 0 ] } );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = sdm.getId();
        assertEquals(expected, tapeService.attain( tape.getId() ).getStorageDomainMemberId(), "Shoulda successfully imported tape.");
        assertTrue(tapeService.attain( tape.getId() ).isAssignedToStorageDomain(), "Shoulda successfully imported tape.");
        assertNotNull(tapeService.attain( tape.getId() ).getBucketId(), "Shoulda successfully imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been a failure.");
    }


    @Test
    public void testImportWhenMultipleBucketsOnTapeAndDataPolicyHasBucketIsolationNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED,
                dataPolicy.getId(),
                DataPersistenceRuleType.PERMANENT,
                storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    @Test
    public void testImportWhenChecksumTypeOfBlobsDoNotMatchThatOfDataPolicyNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        mockDaoDriver.updateBean(
                dataPolicy.setChecksumType( ChecksumType.SHA_512 ), DataPolicy.CHECKSUM_TYPE );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        mockDaoDriver.createBlobs( o44.getId(), 3, 4000, 10 );
        mockDaoDriver.createBlobs( o43.getId(), 3, 4000, 10 );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setId( o44.getId() );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    @Test
    public void testImportWhenSimulatedBadVerifyButRetryWorksDoesImportTape()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final BlobIoFailures blobIoFailures = BeanFactory.newBean( BlobIoFailures.class );
        blobIoFailures.setFailures( new BlobIoFailure [] { BeanFactory.newBean( BlobIoFailure.class ) } );
        tapeDriveResource.setVerifyDataResult( blobIoFailures );
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertFalse(tape.getId().equals(
                        tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId() ), "Should notta successfully imported tape.");
        assertEquals(TapeFailureType.IMPORT_FAILED_DUE_TO_DATA_INTEGRITY, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain(
                        Require.nothing() ).getType(), "Shoulda been a failures.");

        blobIoFailures.setFailures( new BlobIoFailure [] {} );
        tapeDriveResource.setVerifyDataResult( blobIoFailures );
        tapeDriveResource.setDs3Contents( "some_handle", constructResponses( Integer.MAX_VALUE ) );

        dbSupport.getDataManager().createBean( directive );
        mockDaoDriver.updateBean( tape.setState( TapeState.IMPORT_PENDING ), Tape.STATE );
        task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda successfully imported tape.");
        final Object expected = tape.getId();
        assertEquals(expected, tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId(), "Shoulda successfully imported tape.");

        verifyEverythingHasBeenImported( dbSupport.getServiceManager() );
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta been any additional failures.");
    }


    @Test
    public void testFailedImportsEventuallyResultInTaskInvalidation()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        final BlobIoFailures blobIoFailures = BeanFactory.newBean( BlobIoFailures.class );
        blobIoFailures.setFailures( new BlobIoFailure [] { BeanFactory.newBean( BlobIoFailure.class ) } );
        tapeDriveResource.setVerifyDataResult( blobIoFailures );
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        assertNotNull(task.getTapeId(), "Shoulda selected tape to execute against.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda given up.");
        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Shoulda given up.");
        assertEquals(TapeFailureType.IMPORT_FAILED_DUE_TO_DATA_INTEGRITY, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain(
                        Require.nothing() ).getType(), "Shoulda been a failures.");

        dbSupport.getDataManager().createBean( directive );
        mockDaoDriver.updateBean( tape.setState( TapeState.IMPORT_PENDING ), Tape.STATE );
        task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        assertNotNull(task.getTapeId(), "Shoulda selected tape to execute against.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda given up.");
        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Shoulda given up.");
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been an additional failure.");
        final Object expected1 = CollectionFactory.toSet(
                TapeFailureType.IMPORT_FAILED_DUE_TO_DATA_INTEGRITY,
                TapeFailureType.IMPORT_FAILED );
        assertEquals(expected1, BeanUtils.extractPropertyValues(
                        dbSupport.getServiceManager().getRetriever( TapeFailure.class ).retrieveAll().toSet(),
                        com.spectralogic.s3.common.dao.domain.shared.Failure.TYPE ), "Shoulda been multiple failure types.");

        blobIoFailures.setFailures( new BlobIoFailure [] {} );
        tapeDriveResource.setVerifyDataResult( blobIoFailures );
        tapeDriveResource.setTakeOwnershipException( new RuntimeException( "oops" ) );
        tapeDriveResource.setDs3Contents( "somehandle", constructResponses( Integer.MAX_VALUE ) );

        dbSupport.getDataManager().createBean( directive );
        mockDaoDriver.updateBean( tape.setState( TapeState.IMPORT_PENDING ), Tape.STATE );
        task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        assertNotNull(task.getTapeId(), "Shoulda selected tape to execute against.");

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(BlobStoreTaskState.COMPLETED, task.getState(), "Shoulda given up.");
        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Shoulda given up.");
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been an additional failure.");
        final Object expected = CollectionFactory.toSet(
                TapeFailureType.IMPORT_FAILED_DUE_TO_DATA_INTEGRITY,
                TapeFailureType.IMPORT_FAILED,
                TapeFailureType.IMPORT_FAILED_DUE_TO_TAKE_OWNERSHIP_FAILURE );
        assertEquals(expected, BeanUtils.extractPropertyValues(
                        dbSupport.getServiceManager().getRetriever( TapeFailure.class ).retrieveAll().toSet(),
                        com.spectralogic.s3.common.dao.domain.shared.Failure.TYPE ), "Shoulda been multiple failure types.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(ImportTapeDirective.class).getCount(
                Require.nothing()), "Shoulda whacked import directive from db.");
    }


    @Test
    public void testTaskCannotBeRunAgainstTapeNotInImportPendingState()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.FOREIGN );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, BlobStoreTaskNoLongerValidException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
            }
        } );
    }


    @Test
    public void testImportWhenOverlappingConflictingObjectPartsDueToObjectIdConflictFails()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o44.getId(), 3, 2000, 1000 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o43.getId(), 3, 2000, 1000 );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "143" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "144" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "149" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "150" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 1 ].setId(
                blobs3.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 2 ].setId(
                blobs3.get( 1 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 2 ].setId(
                blobs4.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 3 ].setId(
                blobs4.get( 1 ).getId() );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta imported tape.");
        assertFalse(tape.getId().equals(
                        tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId() ), "Should notta imported tape.");
        assertEquals(TapeFailureType.IMPORT_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain(
                        Require.nothing() ).getType(), "Shoulda been a failure.");
    }


    @Test
    public void testImportWhenOverlappingConflictingObjectPartsDueToBlobIdConflictFails()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o44.getId(), 3, 2000, 1000 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o43.getId(), 3, 2000, 1000 );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "143" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "144" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "149" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "150" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setId( o44.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 1 ].setId(
                blobs3.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 2 ].setId(
                blobs3.get( 1 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 2 ].setId(
                blobs4.get( 0 ).getId() );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta imported tape.");
        assertFalse(tape.getId().equals(
                        tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId() ), "Should notta imported tape.");
        assertEquals(TapeFailureType.IMPORT_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain(
                        Require.nothing() ).getType(), "Shoulda been a failure.");
    }


    @Test
    public void testImportWhenOverlappingConflictingObjectPartsDueToBlobChecksumTypeConflictFails()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o44.getId(), 3, 2000, 1000 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o43.getId(), 3, 2000, 1000 );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "143" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "144" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "149" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 1 ).setChecksumType( ChecksumType.SHA_256 ).setChecksum( "150" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setId( o44.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 1 ].setId(
                blobs3.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 2 ].setId(
                blobs3.get( 1 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 2 ].setId(
                blobs4.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 3 ].setId(
                blobs4.get( 1 ).getId() );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta imported tape.");
        assertFalse(tape.getId().equals(
                        tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId() ), "Should notta imported tape.");
        assertEquals(TapeFailureType.IMPORT_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain(
                        Require.nothing() ).getType(), "Shoulda been a failure.");
    }


    @Test
    public void testImportWhenOverlappingConflictingObjectPartsDueToBlobChecksumConflictFails()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o44.getId(), 3, 2000, 1000 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o43.getId(), 3, 2000, 1000 );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "143" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "144" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "149" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "1509" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setId( o44.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 1 ].setId(
                blobs3.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 2 ].setId(
                blobs3.get( 1 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 2 ].setId(
                blobs4.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 3 ].setId(
                blobs4.get( 1 ).getId() );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta imported tape.");
        assertFalse(tape.getId().equals(
                        tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId() ), "Should notta imported tape.");
        assertEquals(TapeFailureType.IMPORT_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain(
                        Require.nothing() ).getType(), "Shoulda been a failure.");
    }


    @Test
    public void testImportWhenOverlappingConflictingObjectPartsDueToBlobLengthConflictFails()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        final List< Blob > blobs4 = mockDaoDriver.createBlobs( o44.getId(), 3, 2000, 1000 );
        final List< Blob > blobs3 = mockDaoDriver.createBlobs( o43.getId(), 3, 2000, 1000 );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "143" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs3.get( 1 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "144" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 0 ).setChecksumType( ChecksumType.MD5 ).setChecksum( "149" ),
                ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        dbSupport.getServiceManager().getService( BlobService.class ).update(
                blobs4.get( 1 ).setLength( 999 )
                                     .setChecksumType( ChecksumType.MD5 ).setChecksum( "150" ),
                Blob.LENGTH, ChecksumObservable.CHECKSUM_TYPE, ChecksumObservable.CHECKSUM );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].setId( o43.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].setId( o44.getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 1 ].setId(
                blobs3.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 3 ].getBlobs()[ 2 ].setId(
                blobs3.get( 1 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 2 ].setId(
                blobs4.get( 0 ).getId() );
        responses.get( 0 ).getBuckets()[ 4 ].getObjects()[ 4 ].getBlobs()[ 3 ].setId(
                blobs4.get( 1 ).getId() );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta imported tape.");
        assertFalse(tape.getId().equals(
                        tapeDriveResource.getLoadedTapeInformation().getWithoutBlocking().getTapeId() ), "Should notta imported tape.");
        assertEquals(TapeFailureType.IMPORT_FAILED, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain(
                        Require.nothing() ).getType(), "Shoulda been a failure.");
    }


    @Test
    public void testImportAnotherVersionUponResolutionModeAcceptExistingNotAllowedWhenNoDataPolicyVersioning()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPolicy.class ).setVersioning( VersioningLevel.NONE ),
                DataPolicy.VERSIONING );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( S3Object.class ).setCreationDate( new Date( 100 ) ),
                S3Object.CREATION_DATE );
        mockDaoDriver.createBlobs( o44.getId(), 3, 2000, 1000 );
        mockDaoDriver.createBlobs( o43.getId(), 3, 2000, 1000 );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(
                Require.nothing()), "Shoulda been a failure.");
    }


    @Test
    public void testImportAnotherVersionAllowedWithKeepLatest()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPolicy.class ).setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( S3Object.class ).setCreationDate( new Date( 100 ) ),
                S3Object.CREATION_DATE );
        final List< Blob > existingBlobs = mockDaoDriver.createBlobs( o44.getId(), 3, 2000, 1000 );
        existingBlobs.addAll( mockDaoDriver.createBlobs( o43.getId(), 3, 2000, 1000 ) );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape existingTape = mockDaoDriver.createTape();
        for ( final Blob b : existingBlobs )
        {
        	mockDaoDriver.putBlobOnTapeAndDetermineStorageDomain( existingTape.getId(), b.getId() );
        }
        dbSupport.getServiceManager().getService( S3ObjectService.class).deleteLegacyObjectsIfEntirelyPersisted( CollectionFactory.toSet(o44.getId()));
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "Shoulda imported tape.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(
                Require.nothing()), "Should notta been a failure.");
        assertNotNull(tapeService.attain( tape.getId() ).getStorageDomainMemberId(), "Shoulda assigned tape to correct storage domain member.");
        final Object expected = tapeService.attain( existingTape.getId() ).getStorageDomainMemberId();
        assertEquals(expected, tapeService.attain( tape.getId() ).getStorageDomainMemberId(), "Shoulda assigned tape to correct storage domain member.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(
                S3Object.NAME, "o_4blobs_4present"), "Shoulda been 1 version of o_4blobs_4present in the end");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(
                S3Object.NAME, "o_4blobs_3present"), "Shoulda been 1 version of o_4blobs_3present in the end");
        assertFalse(o44.getId().equals(
                        dbSupport.getServiceManager().getRetriever( S3Object.class ).attain(
                                S3Object.NAME, "o_4blobs_4present" ).getId() ), "Shoulda whacked original o_4blobs_4present replacing it with the new");
        assertFalse(o43.getId().equals(
                        dbSupport.getServiceManager().getRetriever( S3Object.class ).attain(
                                S3Object.NAME, "o_4blobs_3present" ).getId() ), "Shoulda whacked original o_4blobs_3present replacing it with the new");
    }


    @Test
    public void testImportEmptyTapeNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        mockDaoDriver.createBucket( null, "b_2" );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( DataPolicy.class ).setVersioning( VersioningLevel.KEEP_LATEST ),
                DataPolicy.VERSIONING );
        final Bucket bucket = mockDaoDriver.createBucket( null, "b_5" );
        final S3Object o44 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_4present", -1 );
        final S3Object o43 = mockDaoDriver.createObject( bucket.getId(), "o_4blobs_3present", -1 );
        mockDaoDriver.updateAllBeans(
                BeanFactory.newBean( S3Object.class ).setCreationDate( new Date( 100 ) ),
                S3Object.CREATION_DATE );
        mockDaoDriver.createBlobs( o44.getId(), 3, 2000, 1000 );
        mockDaoDriver.createBlobs( o43.getId(), 3, 2000, 1000 );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = new ArrayList<>();
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(
                Require.nothing()), "Shoulda been a failure.");
    }


    @Test
    public void testImportWhenLtfsFileNamingMismatchBetweenStorageDomainAndTapeNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        mockDaoDriver.updateBean(
                storageDomain.setLtfsFileNaming( LtfsFileNamingMode.OBJECT_NAME ),
                StorageDomain.LTFS_FILE_NAMING );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    @Test
    public void testImportWhenLtfsFileNamingNotPresentOnTapeNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain.getId() );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setLtfsFileNamingMode( null );
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );

        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
    }


    @Test
    public void testImportWhenDataPolicyToUseForNewBucketsHasNoPersistenceRulesNotAllowed()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy(
                MockDaoDriver.DEFAULT_DATA_POLICY_NAME );
        final User user = mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final StorageDomain storageDomain =
                mockDaoDriver.createStorageDomain( MockDaoDriver.DEFAULT_STORAGE_DOMAIN_NAME );
        final TapePartition tapePartition =
                mockDaoDriver.createTapePartition( null, MockDaoDriver.DEFAULT_PARTITION_SN );
        mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO5 );
        final Tape tape = mockDaoDriver.createTape();
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        tapeService.transistState( tape, TapeState.IMPORT_PENDING );
        mockDaoDriver.createTapeDrive( null, "a", tape.getId() );
        final ImportTapeDirective directive =
                mockDaoDriver.createImportTapeDirective( tape.getId(), user.getId(), dataPolicy.getId() );

        final List< S3ObjectsOnMedia > responses = constructResponses( Integer.MAX_VALUE );
        final MockTapeDriveResource tapeDriveResource = new MockTapeDriveResource();
        tapeDriveResource.setDs3Contents( "some_handle", responses );
        tapeDriveResource.setTapeId( UUID.randomUUID() );
        final ImportTapeTask task = createTask( directive.getTapeId(), dbSupport.getServiceManager() );
        task.prepareForExecutionIfPossible( tapeDriveResource, new MockTapeAvailability() );
        
        TestUtil.invokeAndWaitUnchecked( task );

        assertEquals(TapeState.FOREIGN, tapeService.attain( tape.getId() ).getState(), "Should notta successfully imported tape.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda been a failure.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(Bucket.class).getCount(), "Should notta been any buckets created.");
    }
    
    
    private void verifyEverythingHasBeenImported( final BeansServiceManager serviceManager )
    {
        verifyEverythingHasBeenImported( serviceManager, 20, true );
    }
    
    
    private void verifyEverythingHasBeenImported(
            final BeansServiceManager serviceManager,
            final int numBlobTapesExpected,
            final boolean verifyOrder )
    {
        final Bucket b2 = serviceManager.getRetriever( Bucket.class ).attain(
                Bucket.NAME, "b_2" );
        final Bucket b3 = serviceManager.getRetriever( Bucket.class ).attain( 
                Bucket.NAME, "b_3" );
        final S3Object o00 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_0blobs_0present" ); // b_1
        final S3Object o10 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_1blobs_0present" ); // b_2
        final S3Object o11 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_1blobs_1present" ); // b_2
        final S3Object o20 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_2blobs_0present" ); // b_3
        final S3Object o21 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_2blobs_1present" ); // b_3
        final S3Object o22 = serviceManager.getRetriever( S3Object.class ).attain(
                S3Object.NAME, "o_2blobs_2present" ); // b_3
        assertEquals(2,  serviceManager.getRetriever(S3Object.class).getCount(
                Require.beanPropertyEquals(S3Object.BUCKET_ID, b2.getId())), "Shoulda imported all contents.");
        assertEquals(3,  serviceManager.getRetriever(S3Object.class).getCount(
                Require.beanPropertyEquals(S3Object.BUCKET_ID, b3.getId())), "Shoulda imported all contents.");

        assertEquals(0,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o00.getId())), "Shoulda imported all contents.");
        assertEquals(0,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o10.getId())), "Shoulda imported all contents.");
        assertEquals(1,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o11.getId())), "Shoulda imported all contents.");
        assertEquals(0,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o20.getId())), "Shoulda imported all contents.");
        assertEquals(1,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o21.getId())), "Shoulda imported all contents.");
        assertEquals(2,  serviceManager.getRetriever(Blob.class).getCount(
                Require.beanPropertyEquals(Blob.OBJECT_ID, o22.getId())), "Shoulda imported all contents.");

        final Object expected2 = b3.getId();
        assertEquals(expected2, o20.getBucketId(), "Shoulda imported all contents.");
        final Object expected1 = b3.getId();
        assertEquals(expected1, o21.getBucketId(), "Shoulda imported all contents.");
        final Object expected = b3.getId();
        assertEquals(expected, o22.getBucketId(), "Shoulda imported all contents.");

        assertEquals(108000,  o20.getCreationDate().getTime(), "Shoulda imported all contents.");
        assertEquals(110000,  o21.getCreationDate().getTime(), "Shoulda imported all contents.");
        assertEquals(113000,  o22.getCreationDate().getTime(), "Shoulda imported all contents.");

        final Object actual2 = serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                S3ObjectProperty.OBJECT_ID, o20.getId() ).getKey();
        assertEquals(S3HeaderType.ETAG.getHttpHeaderName(), actual2, "Shoulda imported all contents.");
        assertEquals("109", serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                        S3ObjectProperty.OBJECT_ID, o20.getId() ).getValue(), "Shoulda imported all contents.");
        final Object actual1 = serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                S3ObjectProperty.OBJECT_ID, o21.getId() ).getKey();
        assertEquals(S3HeaderType.ETAG.getHttpHeaderName(), actual1, "Shoulda imported all contents.");
        assertEquals("111", serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                        S3ObjectProperty.OBJECT_ID, o21.getId() ).getValue(), "Shoulda imported all contents.");
        final Object actual = serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                S3ObjectProperty.OBJECT_ID, o22.getId() ).getKey();
        assertEquals(S3HeaderType.ETAG.getHttpHeaderName(), actual, "Shoulda imported all contents.");
        assertEquals("114", serviceManager.getRetriever( S3ObjectProperty.class ).attain(
                        S3ObjectProperty.OBJECT_ID, o22.getId() ).getValue(), "Shoulda imported all contents.");

        final List< Blob > blobs = new ArrayList<>( BeanUtils.sort(
                serviceManager.getRetriever( Blob.class ).retrieveAll( 
                        Blob.OBJECT_ID, o22.getId() ).toSet() ) );
        assertEquals(2,  blobs.size(), "Shoulda imported all contents.");
        assertEquals(0,  blobs.get(0).getByteOffset(), "Shoulda imported all contents.");
        assertEquals(1000,  blobs.get(0).getLength(), "Shoulda imported all contents.");
        assertEquals("115", blobs.get( 0 ).getChecksum(), "Shoulda imported all contents.");
        assertEquals(ChecksumType.MD5, blobs.get( 0 ).getChecksumType(), "Shoulda imported all contents.");
        assertEquals(1000,  blobs.get(1).getByteOffset(), "Shoulda imported all contents.");
        assertEquals(1000,  blobs.get(1).getLength(), "Shoulda imported all contents.");
        assertEquals("116", blobs.get( 1 ).getChecksum(), "Shoulda imported all contents.");
        assertEquals(ChecksumType.MD5, blobs.get( 1 ).getChecksumType(), "Shoulda imported all contents.");

        assertEquals(0,  serviceManager.getRetriever(ImportTapeDirective.class).getCount(Require.nothing()), "Shoulda whacked import directive from db.");
        assertEquals(numBlobTapesExpected,  serviceManager.getRetriever(BlobTape.class).getCount(Require.nothing()), "Shoulda recorded blobs on tape.");

        if ( verifyOrder )
        {
            verifyOrder( serviceManager );
        }
    }
    
    
    private void verifyOrder( final BeansServiceManager serviceManager )
    {
        final List< Blob > sortedBlobs = serviceManager.getRetriever( Blob.class ).retrieveAll().toList();
        Collections.sort( sortedBlobs, new BeanComparator<>( Blob.class, ChecksumObservable.CHECKSUM ) );
        
        final Map< UUID, Integer > orderIndexes = new HashMap<>();
        for ( final BlobTape bt : serviceManager.getRetriever( BlobTape.class ).retrieveAll().toSet() )
        {
            orderIndexes.put( bt.getBlobId(), Integer.valueOf( bt.getOrderIndex() ) );
        }
        
        int lastOrderIndex = -1;
        for ( final Blob blob : sortedBlobs )
        {
            final Integer orderIndex = orderIndexes.get( blob.getId() );
            if ( null == orderIndex )
            {
                continue;
            }
            if ( orderIndex.intValue() < lastOrderIndex )
            {
                fail( "Blobs imported out-of-order (order indexes should reflect order reported on tape)."
                        + "On blob " + blob.getId() + ", order index was " + orderIndex 
                        + ", but last order index was " + lastOrderIndex + "." );
            }
            lastOrderIndex = orderIndex.intValue();
        }
    }
    
    
    private List< S3ObjectsOnMedia > constructResponses( final int maxBucketsPerResponse )
    {
        return constructResponses( maxBucketsPerResponse, Integer.MAX_VALUE, true, 0, 1, false);
    }
    
    
    private List< S3ObjectsOnMedia > constructResponses(
            final int maxBucketsPerResponse, 
            final int zeroLengthBlobStep,
            final boolean includeTotalBlobCount,
            final int partNumber,
            final int numParts,
            final boolean interleaved )
    {
    	//TODO: refactor - this code is unreadable
        assertFalse(interleaved && maxBucketsPerResponse > 1, "Interleaved construction is only implemented for single bucket responses");
        int checksum = 100;
        final List< S3ObjectOnMedia > ooms = new ArrayList<>();
        for ( int i = 0; i < 5; ++i )
        {
            for ( int j = 0; j <= i; ++j )
            {
                final List< S3ObjectMetadataKeyValue > metadatas = new ArrayList<>();
                metadatas.add( BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                        .setKey( KeyValueObservable.CREATION_DATE )
                        .setValue( String.valueOf( ( ++checksum ) * 1000 ) ) );
                metadatas.add( BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                        .setKey( S3HeaderType.ETAG.getHttpHeaderName() )
                        .setValue( String.valueOf( ++checksum ) ) );
                
                final List< BlobOnMedia > bomsForObject = new ArrayList<>();
                final S3ObjectOnMedia oom = BeanFactory.newBean( S3ObjectOnMedia.class );
                final S3ObjectOnMedia oom2 = BeanFactory.newBean( S3ObjectOnMedia.class );
                
                final String name =  "o_" + String.valueOf( i ) + "blobs_" + String.valueOf( j ) + "present";
                oom.setId( UUID.randomUUID() );
                oom.setObjectName( name );
                
                int skippedBlobCount = 0;
                if ( ( 0 < checksum % zeroLengthBlobStep ) )
                {
                    for ( int k = i - j; k < i; ++k )
                    {
                        final BlobOnMedia bom = BeanFactory.newBean( BlobOnMedia.class );
                        bom.setChecksum( String.valueOf( ++checksum ) );
                        bom.setChecksumType( ChecksumType.MD5 );
                        bom.setId( UUID.randomUUID() );
                        bom.setLength( 1000 );
                        bom.setOffset( k * 1000 );
                        if ( partNumber == k % numParts )
                        {
                            bomsForObject.add( bom );
                        }
                        else
                        {
                            ++skippedBlobCount;
                        }
                    }
                }
                else
                {
                    final BlobOnMedia bom = BeanFactory.newBean( BlobOnMedia.class );
                    bom.setChecksum( String.valueOf( ++checksum ) );
                    bom.setChecksumType( ChecksumType.MD5 );
                    bom.setId( UUID.randomUUID() );
                    bom.setLength( 0 );
                    bom.setOffset( 0 );
                    bomsForObject.add( bom );
                }
                if ( includeTotalBlobCount )
                {
                    metadatas.add( BeanFactory.newBean( S3ObjectMetadataKeyValue.class )
                            .setKey( KeyValueObservable.TOTAL_BLOB_COUNT )
                            .setValue( String.valueOf( bomsForObject.size() + skippedBlobCount ) ) );
                }
                oom.setMetadata( CollectionFactory.toArray( S3ObjectMetadataKeyValue.class, metadatas ) );
                if ( interleaved )
                {
                    oom2.setMetadata(
                            CollectionFactory.toArray( S3ObjectMetadataKeyValue.class, metadatas ) );
                    final List< BlobOnMedia > bomsForObject2 = new ArrayList<>();
                    for (int k = 0; k < bomsForObject.size() / 2; k++ )
                    {
                        bomsForObject2.add( bomsForObject.remove( bomsForObject.size() - 1 ) );
                    }
                    oom.setBlobs( CollectionFactory.toArray( BlobOnMedia.class, bomsForObject ) );
                    oom2.setBlobs( CollectionFactory.toArray( BlobOnMedia.class, bomsForObject2 ) );
                    if ( !bomsForObject2.isEmpty() )
                    {
                        oom2.setId( oom.getId() );
                        oom2.setObjectName( oom.getObjectName() );
                        ooms.add( oom2 );
                    }
                }
                else
                {
                    oom.setBlobs( CollectionFactory.toArray( BlobOnMedia.class, bomsForObject ) );
                }
                ooms.add( oom );
                
            }
        }
        
        final List< S3ObjectOnMedia > oomsInBucket = new ArrayList<>();
        final List< BucketOnMedia > buckets = new ArrayList<>();
        if ( interleaved )
        {
            Collections.shuffle( ooms );
            final BucketOnMedia bucket = BeanFactory.newBean( BucketOnMedia.class );
            bucket.setBucketName( "b_0");
            bucket.setObjects( CollectionFactory.toArray( S3ObjectOnMedia.class, ooms ) );
            buckets.add( bucket );
        }
        else
        {
            for ( final S3ObjectOnMedia oom : ooms )
            {
                oomsInBucket.add( oom );
                if ( buckets.size() < oomsInBucket.size() )
                {
                    final BucketOnMedia bucket = BeanFactory.newBean( BucketOnMedia.class );
                    bucket.setBucketName( "b_" + oomsInBucket.size() );
                    bucket.setObjects( CollectionFactory.toArray( S3ObjectOnMedia.class, oomsInBucket ) );
                    buckets.add( bucket );
                    oomsInBucket.clear();
                }
            }
        }
            
        final List< BucketOnMedia > bucketsInResponse = new ArrayList<>();
        final List< S3ObjectsOnMedia > retval = new ArrayList<>();
        for ( final BucketOnMedia bucket : buckets )
        {
            bucketsInResponse.add( bucket );
            if ( bucketsInResponse.size() == maxBucketsPerResponse )
            {
                final S3ObjectsOnMedia response = BeanFactory.newBean( S3ObjectsOnMedia.class );
                response.setBuckets( CollectionFactory.toArray( BucketOnMedia.class, bucketsInResponse ) );
                retval.add( response );
            }
        }
        if ( !bucketsInResponse.isEmpty() )
        {
            final S3ObjectsOnMedia response = BeanFactory.newBean( S3ObjectsOnMedia.class );
            response.setBuckets( CollectionFactory.toArray( BucketOnMedia.class, bucketsInResponse ) );
            retval.add( response );
            bucketsInResponse.clear();
        }
        
        return retval;
    }
    
    
    private BlobStore getBlobStore()
    {
        return InterfaceProxyFactory.getProxy( BlobStore.class, null );
    }

    private ImportTapeTask createTask( UUID tapeId, BeansServiceManager serviceManager ) {
        return new ImportTapeTask(
                BlobStoreTaskPriority.values()[ 0 ],
                tapeId,
                getBlobStore(),
                new MockDiskManager( serviceManager ),
                new TapeFailureManagement(serviceManager), serviceManager );
    }


    private ImportTapeTask createTask(UUID tapeId, BeansServiceManager serviceManager, BasicTestsInvocationHandler blobStoreBtih) {
        return new ImportTapeTask(
                BlobStoreTaskPriority.values()[ 0 ],
                tapeId,
                InterfaceProxyFactory.getProxy( BlobStore.class, blobStoreBtih ),
                new MockDiskManager( serviceManager ),
                new TapeFailureManagement(serviceManager), serviceManager );
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }

}
