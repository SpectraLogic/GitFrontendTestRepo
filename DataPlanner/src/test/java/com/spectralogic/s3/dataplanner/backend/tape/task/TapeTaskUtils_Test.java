/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape.task;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.rpc.tape.TapeResourceFailureCode;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.tape.task.TapeTaskUtils.FailureHandling;
import com.spectralogic.s3.dataplanner.backend.tape.task.TapeTaskUtils.RestoreExpected;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.client.RpcTimeoutException;
import com.spectralogic.util.net.rpc.domain.Failure;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public final class TapeTaskUtils_Test 
{

    @Test
    public void testVerifyQuiescedToCheckpointNullTapeNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint( 
                        null, 
                        rpcResource, 
                        dbSupport.getServiceManager(),
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                        FailureHandling.THROW_EXCEPTION );
            }
        } );
    }
    

    @Test
    public void testVerifyQuiescedToCheckpointNullRpcResourceNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint( 
                        tape, 
                        null, 
                        dbSupport.getServiceManager(),
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                        FailureHandling.THROW_EXCEPTION );
            }
        } );
    }
    

    @Test
    public void testVerifyQuiescedToCheckpointNullServiceManagerNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint( 
                        tape, 
                        rpcResource, 
                        null,
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                        FailureHandling.THROW_EXCEPTION );
            }
        } );
    }
    

    @Test
    public void testVerifyQuiescedToCheckpointNullFailureHandlingNotAllowed()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint( 
                        tape, 
                        rpcResource, 
                        dbSupport.getServiceManager(),
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                        null );
            }
        } );
    }
    

    @Test
    public void testVerifyQuiescedToCheckpointWhenFailToGetLoadedTapeInformationDoesNothing()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setFailGetLoadedTapeInformation( true );
        rpcResource.setTapeId( tape.getId() );
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint( 
                        tape, 
                        rpcResource, 
                        dbSupport.getServiceManager(),
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                        FailureHandling.THROW_EXCEPTION );
            }
        } );
        assertEquals(TapeFailureType.GET_TAPE_INFORMATION_FAILED, mockDaoDriver.attainOneAndOnly( TapeFailure.class ).getType(), "Shoulda created tape failure.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        rpcResource.setFailGetLoadedTapeInformation( true );
        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(2, dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda created tape failure.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
    }
    

    @Test
    public void testVerifyQuiescedToCheckpointWhenTapeUnmodifiedDoesNothing()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.THROW_EXCEPTION );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
    }
    
    
    @Test
    public void testVerifyQuiescedToCheckpointWhenTapeHasNoLastCheckpointDoesNothing()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.FOREIGN );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        assertEquals(null, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.THROW_EXCEPTION );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals(null, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.FOREIGN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals(null, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.FOREIGN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
    }
    

    @Test
    public void testVerifyQuiescedToCheckpointReturnsNewCheckpointResultsInHandlingForCheckpointRestore()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        rpcResource.setVerifyQuiescedToCheckpointResponse( "update" );
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.NO,
                FailureHandling.THROW_EXCEPTION );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals("update", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda updated checkpoint to functionally equivalent checkpoint returned by backend.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        tape.setAllowRollback(true);
        rpcResource.setVerifyQuiescedToCheckpointResponse( "roll" );
        TapeTaskUtils.verifyQuiescedToCheckpoint(
                tape,
                rpcResource,
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.NO,
                FailureHandling.THROW_EXCEPTION );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals("roll", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                        .getLastCheckpoint(), "Shoulda updated to new checkpoint because a quiesce had been in progress.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
        assertFalse(dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).isAllowRollback(), "Tape should no longer be quiesce in progress");

        rpcResource.setVerifyQuiescedToCheckpointResponse( "roll2" );
        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created tape failure for checkpoint rollback.");
        assertEquals("roll2", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda updated checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
    }
    
    
    @Test
    public void testVerifyQuiescedToCheckpointThrowsNotFoundResultsInHandlingForCheckpointNotFound()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        rpcResource.setVerifyQuiescedToCheckpointException( new RpcProxyException(
                "blah",
                BeanFactory.newBean( Failure.class )
                    .setCode( TapeResourceFailureCode.CHECKPOINT_NOT_FOUND.toString() )
                    .setHttpResponseCode( GenericFailure.NOT_FOUND.getHttpResponseCode() )
                    .setMessage( "blah" ) ) );
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint( 
                        tape, 
                        rpcResource, 
                        dbSupport.getServiceManager(),
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                        FailureHandling.THROW_EXCEPTION );
            }
        } );
        assertEquals(TapeFailureType.DATA_CHECKPOINT_MISSING, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain( Require.nothing() )
                    .getType(), "Shoulda created tape failure for checkpoint rollback.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.DATA_CHECKPOINT_MISSING, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda created tape failure for checkpoint rollback.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.DATA_CHECKPOINT_MISSING, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
    }
    
    
    @Test
    public void testVerifyQuiescedToCheckpointThrowsBadDriveStateResultsInHandlingForCheckpointNotFound()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        rpcResource.setVerifyQuiescedToCheckpointException( new RpcProxyException(
                "blah",
                BeanFactory.newBean( Failure.class )
                    .setCode( TapeResourceFailureCode.BAD_DRIVE_STATE.toString() )
                    .setHttpResponseCode( GenericFailure.CONFLICT.getHttpResponseCode() )
                    .setMessage( "blah" ) ) );
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint( 
                        tape, 
                        rpcResource, 
                        dbSupport.getServiceManager(),
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                        FailureHandling.THROW_EXCEPTION );
            }
        } );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created tape failure for bad drive state on verify quiesce attempt.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created tape failure.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
    }
    
    @Test
    public void testVerifyQuiescedToCheckpointThrowsBadDriveStateResults()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean( tape.setTakeOwnershipPending( true ), Tape.TAKE_OWNERSHIP_PENDING );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        rpcResource.setVerifyQuiescedToCheckpointException( new RpcProxyException(
                "blah",
                BeanFactory.newBean( Failure.class )
                    .setCode( TapeResourceFailureCode.BAD_DRIVE_STATE.toString() )
                    .setHttpResponseCode( GenericFailure.CONFLICT.getHttpResponseCode() )
                    .setMessage( "blah" ) ) );
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint( 
                        tape, 
                        rpcResource, 
                        dbSupport.getServiceManager(),
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                        FailureHandling.THROW_EXCEPTION );
            }
        } );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created tape failure for bad drive state on verify quiesce attempt.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        mockDaoDriver.updateBean( tape.setTakeOwnershipPending( true ), Tape.TAKE_OWNERSHIP_PENDING );
        mockDaoDriver.updateBean( tape.setState( TapeState.NORMAL ), Tape.STATE );
        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created tape failure.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
    }
    
    
    @Test
    public void testVerifyQuiescedToCheckpointThrowsUnknownErrorResultsInHandlingForCheckpointVerifyFailure()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        rpcResource.setVerifyQuiescedToCheckpointException( new RpcProxyException(
                "blah",
                BeanFactory.newBean( Failure.class ) ) );
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint( 
                        tape, 
                        rpcResource, 
                        dbSupport.getServiceManager(),
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                        FailureHandling.THROW_EXCEPTION );
            }
        } );
        assertEquals(TapeFailureType.DATA_CHECKPOINT_FAILURE, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain( Require.nothing() )
                    .getType(), "Shoulda created tape failure for checkpoint rollback.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.DATA_CHECKPOINT_FAILURE, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda created tape failure for checkpoint rollback.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.DATA_CHECKPOINT_FAILURE, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda modified tape's state.");
    }


    @Test
    public void testVerifyQuiescedToCheckpointThrowsDataLossErrorResultsInHandlingForCheckpointVerifyFailure()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        rpcResource.setVerifyQuiescedToCheckpointException( new RpcProxyException(
                "blah",
                BeanFactory.newBean( Failure.class )
                        .setCode( TapeResourceFailureCode.CHECKPOINT_DATA_LOSS.toString() )
                        .setHttpResponseCode( GenericFailure.CONFLICT.getHttpResponseCode() )
                        .setMessage( "blah" ) ) );;
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                        .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint(
                        tape,
                        rpcResource,
                        dbSupport.getServiceManager(),
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.NO,
                        FailureHandling.THROW_EXCEPTION );
            }
        } );
        assertEquals(TapeFailureType.DATA_CHECKPOINT_FAILURE, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain( Require.nothing() )
                        .getType(), "Shoulda created tape failure for checkpoint rollback.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                        .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.DATA_CHECKPOINT_FAILURE, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        TapeTaskUtils.verifyQuiescedToCheckpoint(
                tape,
                rpcResource,
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda created tape failure for checkpoint rollback.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                        .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.DATA_CHECKPOINT_FAILURE, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda modified tape's state.");
    }


    @Test
    public void testVerifyQuiescedToCheckpointThrowsRollbackTooFarErrorResultsInHandlingForCheckpointVerifyFailure()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        rpcResource.setVerifyQuiescedToCheckpointException( new RpcProxyException(
                "blah",
                BeanFactory.newBean( Failure.class )
                        .setCode( TapeResourceFailureCode.CHECKPOINT_ROLLBACK_TOO_FAR.toString() )
                        .setHttpResponseCode( GenericFailure.CONFLICT.getHttpResponseCode() )
                        .setMessage( "blah" ) ) );;
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                        .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint(
                        tape,
                        rpcResource,
                        dbSupport.getServiceManager(),
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                        FailureHandling.THROW_EXCEPTION );
            }
        } );
        assertEquals(TapeFailureType.DATA_CHECKPOINT_FAILURE, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain( Require.nothing() )
                        .getType(), "Shoulda created tape failure for checkpoint rollback.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                        .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.DATA_CHECKPOINT_FAILURE, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        TapeTaskUtils.verifyQuiescedToCheckpoint(
                tape,
                rpcResource,
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda created tape failure for checkpoint rollback.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                        .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.DATA_CHECKPOINT_FAILURE, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Shoulda modified tape's state.");
    }
    
    
    @Test
    public void testVerifyQuiescedToCheckpointThrowsUnknownErrorResultsInHandlingWhenTapeIsReadOnly()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean( tape.setWriteProtected( true ), Tape.WRITE_PROTECTED );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        rpcResource.setVerifyQuiescedToCheckpointException( new RpcProxyException(
                "blah",
                BeanFactory.newBean( Failure.class ) ) );
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint( 
                        tape, 
                        rpcResource, 
                        dbSupport.getServiceManager(),
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                        FailureHandling.THROW_EXCEPTION );
            }
        } );
        assertEquals(TapeFailureType.DATA_CHECKPOINT_FAILURE_DUE_TO_READ_ONLY, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain( Require.nothing() )
                    .getType(), "Shoulda created tape failure for checkpoint rollback.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.DATA_CHECKPOINT_FAILURE_DUE_TO_READ_ONLY, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda created tape failure for checkpoint rollback.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.DATA_CHECKPOINT_FAILURE_DUE_TO_READ_ONLY, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
    }
    
    
    @Test
    public void testVerifyQuiescedToCheckpointThrowsUnserviceableResultsInHandlingForCheckpointVerifyFailure()
            throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException,
                   IllegalArgumentException, InvocationTargetException
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        final Constructor< RpcTimeoutException > rpcExceptionConstructor = 
                RpcTimeoutException.class.getDeclaredConstructor( String.class );
        rpcExceptionConstructor.setAccessible( true );
        rpcResource.setVerifyQuiescedToCheckpointException( 
                rpcExceptionConstructor.newInstance( "blah" ) );
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint( 
                        tape, 
                        rpcResource, 
                        dbSupport.getServiceManager(),
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                        FailureHandling.THROW_EXCEPTION );
            }
        } );
        assertEquals(TapeFailureType.DATA_CHECKPOINT_FAILURE, dbSupport.getServiceManager().getRetriever( TapeFailure.class ).attain( Require.nothing() )
                    .getType(), "Shoulda created tape failure for checkpoint rollback.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda created tape failure for checkpoint rollback.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Should notta updated checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
    }
    

    @Test
    public void testFTVerifyQuiescedToCheckpointWhenTapeUnmodifiedDoesNothing()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( UUID.randomUUID() );
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.THROW_EXCEPTION );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
    }
    
    @Test
    public void testFTVerifyQuiescedToCheckpointWhenTapeUnmodified()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean( tape.setTakeOwnershipPending( true ), Tape.TAKE_OWNERSHIP_PENDING );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( UUID.randomUUID() );
        rpcResource.setLoadedTapeReadOnly( true );
        rpcResource.setHasChangedSinceCheckpointResponse( false );
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.THROW_EXCEPTION );
        mockDaoDriver.attainAndUpdate( tape );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals("new", tape.getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.NORMAL, tape.getState(), "Should notta modified tape's state.");
        assertEquals(true, tape.isTakeOwnershipPending(), "Should notta modified tape's state.");

        rpcResource.setLoadedTapeReadOnly( false );
        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.THROW_EXCEPTION );
        mockDaoDriver.attainAndUpdate( tape );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals("blank", tape.getLastCheckpoint(), "Shoulda updated checkpoint.");
        assertEquals(TapeState.NORMAL, tape.getState(), "Should notta modified tape's state.");
        assertEquals(false, tape.isTakeOwnershipPending(), "Shoulda modified tape's state.");
    }
    

    @Test
    public void testFTVerifyQuiescedToCheckpointWhenTapeModifiedRequiresReimportIfTapeWriteProtected()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean( tape.setTakeOwnershipPending( true ), Tape.TAKE_OWNERSHIP_PENDING );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( UUID.randomUUID() );
        rpcResource.setLoadedTapeReadOnly( true );
        rpcResource.setHasChangedSinceCheckpointResponse( true );
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.THROW_EXCEPTION );
        mockDaoDriver.attainAndUpdate( tape );
        assertEquals(TapeFailureType.REIMPORT_REQUIRED, mockDaoDriver.attainOneAndOnly( TapeFailure.class ).getType(), "Shoulda created a tape failure for re-import.");
        assertEquals(null, tape.getLastCheckpoint(), "Shoulda whacked original checkpoint.");
        assertEquals(TapeState.FOREIGN, tape.getState(), "Shoulda modified tape's state.");
        assertEquals(false, tape.isTakeOwnershipPending(), "Shoulda modified tape's state.");
    }
    

    @Test
    public void testFTVerifyQuiescedToCheckpointWhenTapeModifiedRequiresReimportIfTapeNotWriteProtected()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean( tape.setTakeOwnershipPending( true ), Tape.TAKE_OWNERSHIP_PENDING );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( UUID.randomUUID() );
        rpcResource.setLoadedTapeReadOnly( false );
        rpcResource.setHasChangedSinceCheckpointResponse( true );
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.THROW_EXCEPTION );
        mockDaoDriver.attainAndUpdate( tape );
        assertEquals(TapeFailureType.REIMPORT_REQUIRED, mockDaoDriver.attainOneAndOnly( TapeFailure.class ).getType(), "Shoulda created a tape failure for re-import.");
        assertEquals(null, tape.getLastCheckpoint(), "Shoulda whacked original checkpoint.");
        assertEquals(TapeState.FOREIGN, tape.getState(), "Shoulda modified tape's state.");
        assertEquals(false, tape.isTakeOwnershipPending(), "Shoulda modified tape's state.");
    }
    
    
    @Test
    public void testFTVerifyQuiescedToCheckpointWhenTapeHasNoLastCheckpointDoesNothing()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.FOREIGN );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( UUID.randomUUID() );
        assertEquals(null, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.THROW_EXCEPTION );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals(null, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.FOREIGN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals(null, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.FOREIGN, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
    }
    
    
    @Test
    public void testLTFSForeignVerifyQuiescedToCheckpointDoesNotTakeOwnership()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.LTFS_WITH_FOREIGN_DATA );
        mockDaoDriver.updateBean( tape.setLastCheckpoint( "new" ), Tape.LAST_CHECKPOINT );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        rpcResource.setTakeOwnershipException(
                new RpcProxyException( "Should notta taken ownership of LTFS foreign tape.",
                        BeanFactory.newBean( Failure.class ) ) );
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.THROW_EXCEPTION );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.LTFS_WITH_FOREIGN_DATA, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .isTakeOwnershipPending(), "Should notta set ownership pending.");


        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.YES,
                FailureHandling.LOG_IT );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.LTFS_WITH_FOREIGN_DATA, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
        assertEquals(false, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .isTakeOwnershipPending(), "Should notta set ownership pending.");
    }
    
    
    @Test
    public void testFTVerifyQuiescedToCheckpointDoesNotVerifyQuiescedToCheckpoint()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( UUID.randomUUID() );
        rpcResource.setVerifyQuiescedToCheckpointException( new RuntimeException( "NO!" ) );
        
        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.NO,
                FailureHandling.THROW_EXCEPTION );
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Should notta created any tape failures.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
    }
    
    
    @Test
    public void testFTVerifyQuiescedToCheckpointOnTapePendOwnHasChangedSinceCheckpointFailsHandled()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean( tape.setTakeOwnershipPending( true ), Tape.TAKE_OWNERSHIP_PENDING );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setLoadedTapeReadOnly( true );
        rpcResource.setTapeId( UUID.randomUUID() );
        rpcResource.setHasChangedSinceCheckpointException( 
                new RpcProxyException( "Oops", BeanFactory.newBean( Failure.class ) ) );

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint( 
                        tape, 
                        rpcResource, 
                        dbSupport.getServiceManager(),
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.NO,
                        FailureHandling.THROW_EXCEPTION );
            }
        } );
        assertEquals(TapeFailureType.DELAYED_OWNERSHIP_FAILURE, mockDaoDriver.attainOneAndOnly( TapeFailure.class ).getType(), "Shoulda created tape failure.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.NO,
                FailureHandling.LOG_IT );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda created tape failure.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        rpcResource.setLoadedTapeReadOnly( false );
        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.NO,
                FailureHandling.LOG_IT );
        assertEquals(3,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda created tape failure.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");
    }
    
    
    @Test
    public void testFTVerifyQuiescedToCheckpointOnTapePendOwnTakeOwnershipFailsHandled()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.updateBean( tape.setTakeOwnershipPending( true ), Tape.TAKE_OWNERSHIP_PENDING );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setLoadedTapeReadOnly( false );
        rpcResource.setTapeId( UUID.randomUUID() );
        rpcResource.setHasChangedSinceCheckpointResponse( false );
        rpcResource.setTakeOwnershipException(
                new RpcProxyException( "Oops", BeanFactory.newBean( Failure.class ) ) );

        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                TapeTaskUtils.verifyQuiescedToCheckpoint( 
                        tape, 
                        rpcResource, 
                        dbSupport.getServiceManager(),
                        new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.NO,
                        FailureHandling.THROW_EXCEPTION );
            }
        } );
        assertEquals(TapeFailureType.DELAYED_OWNERSHIP_FAILURE, mockDaoDriver.attainOneAndOnly( TapeFailure.class ).getType(), "Shoulda created tape failure.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.NO,
                FailureHandling.LOG_IT );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(TapeFailure.class).getCount(), "Shoulda created tape failure.");
        assertEquals("new", dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() )
                    .getLastCheckpoint(), "Shoulda retained original checkpoint.");
        assertEquals(TapeState.NORMAL, dbSupport.getServiceManager().getRetriever( Tape.class ).attain( tape.getId() ).getState(), "Should notta modified tape's state.");

        rpcResource.setLoadedTapeReadOnly( true );
        TapeTaskUtils.verifyQuiescedToCheckpoint( 
                tape, 
                rpcResource, 
                dbSupport.getServiceManager(),
                new TapeFailureManagement(dbSupport.getServiceManager()), RestoreExpected.NO,
                FailureHandling.THROW_EXCEPTION );
    }

    @Test
    public void testMediumSinglePartitionError() {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Tape tape = mockDaoDriver.createTape( TapeState.NORMAL );
        mockDaoDriver.createTapeDrive( null, "tdsn", tape.getId() );
        final MockTapeDriveResource rpcResource = new MockTapeDriveResource();
        rpcResource.setTapeId( tape.getId() );
        rpcResource.setFailPartition( true );

        TestUtil.assertThrows( "Medium contains only one partition", RpcProxyException.class, () -> TapeTaskUtils.inspect(
                tape,
                UUID.randomUUID(),
                rpcResource,
                new TapeFailureManagement(dbSupport.getServiceManager())
        ));
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
