/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.tape;

import java.lang.reflect.Method;
import java.util.List;
import java.util.UUID;

import com.spectralogic.s3.dataplanner.testfrmwrk.DataPlannerIntegrationTester;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeLibrary;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.simulator.Simulator;
import com.spectralogic.s3.simulator.state.SimStateManager;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.net.rpc.client.RpcClientInvocationDiagnostics;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.thread.wp.SystemWorkPool;


import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;


@Tag("rpc-integration")
public final class TapeBlobStoreIntegration_Test
{
    @Test
    public void testRefreshPhysicalTapeEnvironmentNowDoesSo()
    {
        final Simulator simulator = new Simulator(Simulator.getTestConfig());
        final TapeBlobStoreTestSupport tbsSupport = new TapeBlobStoreTestSupport( dbSupport );
        try
        {
            SystemWorkPool.getInstance().submit( simulator );
            final SimStateManager simulatorStateManager = simulator.getStateManager( 20 );
            simulatorStateManager.reset( 1, 2, 6, null );
            
            assertEquals(0, dbSupport.getServiceManager().getRetriever( TapeLibrary.class ).getCount(), "Should notta updated physical tape environment yet.");
            assertEquals(0, dbSupport.getServiceManager().getRetriever( TapeDrive.class ).getCount(), "Should notta updated physical tape environment yet.");
            assertEquals(0, dbSupport.getServiceManager().getRetriever( Tape.class ).getCount(), "Should notta updated physical tape environment yet.");
            
            tbsSupport.getBlobStore().refreshEnvironmentNow();
            
            assertEquals(1, dbSupport.getServiceManager().getRetriever( TapeLibrary.class ).getCount(), "Shoulda updated physical tape environment as a result of the call.");
            assertEquals(2, dbSupport.getServiceManager().getRetriever( TapeDrive.class ).getCount(), "Shoulda updated physical tape environment as a result of the call.");
            assertEquals(6, dbSupport.getServiceManager().getRetriever( Tape.class ).getCount(), "Shoulda updated physical tape environment as a result of the call.");
        }
        finally
        {
            simulator.shutdown();
            tbsSupport.shutdown();
        }
    }
    
    @Test
    public void testTasksScheduledToTapeDriveWhenTapeAlreadyLoadedInItWhenMultipleTapeDrivesAvailable()
    {
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        final TapeDriveService tapeDriveService =
                dbSupport.getServiceManager().getService( TapeDriveService.class );
        final Simulator simulator = new Simulator(Simulator.getTestConfig());
        final TapeBlobStoreTestSupport tbsSupport = new TapeBlobStoreTestSupport( dbSupport );
        try
        {
            SystemWorkPool.getInstance().submit( simulator );
            final SimStateManager simulatorStateManager = simulator.getStateManager( 20 );
            simulatorStateManager.reset( 1, 2, 6, null );
            
            tbsSupport.getBlobStore().refreshEnvironmentNow();
            dbSupport.getDataManager().updateBeans(
                    CollectionFactory.toSet( TapePartition.QUIESCED ),
                    BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ), 
                    Require.nothing() );
            
            RpcClientInvocationDiagnostics.getInstance().enable();
            int c = 0, i = 0; // 100ms X 100 = 10000ms = 10s => Yes, intentional.
            do
            {
                tbsSupport.mockPeriodicStartNewTasksCall();
                TestUtil.sleep( 100 );
                c = tapeService.getCount( Require.beanPropertyEquals( 
                                            Tape.STATE, TapeState.NORMAL ) );
                Logger.getLogger( this.getClass() ).info(
                    "Tapes set to NORMAL by end of iteraton " + i + ": " + c );
                
            } while ( 100 > ++i && 6 != c ) ;
            
            assertEquals(
                    6,
                    tapeService.getCount( Require.beanPropertyEquals(
                            Tape.STATE, TapeState.NORMAL ) ),
                    "All tapes shoulda switched to normal state."
                    );
            
            final List< TapeDrive > drives = tapeDriveService.retrieveAll().toList();
            final Method methodFormat =
                     ReflectUtil.getMethod( TapeDriveResource.class, "format" );
            int totalCallCount = 0;
            
            List< BasicTestsInvocationHandler > btihList =
                    RpcClientInvocationDiagnostics.getInstance().getBtihs(
                                            TapeDriveResource.class, 
                                            drives.get( 0 ).getSerialNumber() );
            for( BasicTestsInvocationHandler b : btihList )
            {
                totalCallCount = totalCallCount + b.getMethodCallCount( methodFormat );
            }
            
            btihList = RpcClientInvocationDiagnostics.getInstance().getBtihs(
                                            TapeDriveResource.class, 
                                            drives.get( 1 ).getSerialNumber() );
            for( BasicTestsInvocationHandler b : btihList )
            {
                totalCallCount = totalCallCount + b.getMethodCallCount( methodFormat );
            }
            
            assertEquals(
                    6,
                    totalCallCount,
               "Each of the tapes shoulda been formatted.");
        }
        finally
        {
            simulator.shutdown();
            tbsSupport.shutdown();
        }
    }
    
    @Test
    public void testTasksScheduledToTapeDriveWhenMultiplePartitions()
    {
        final TapeService tapeService = dbSupport.getServiceManager().getService( TapeService.class );
        final TapeDriveService tapeDriveService =
                dbSupport.getServiceManager().getService( TapeDriveService.class );
        final Simulator simulator = new Simulator(Simulator.getTestConfig());
        final TapeBlobStoreTestSupport tbsSupport = new TapeBlobStoreTestSupport( dbSupport );
        try
        {
            SystemWorkPool.getInstance().submit( simulator );
            final SimStateManager simulatorStateManager = simulator.getStateManager( 20 );
            simulatorStateManager.reset( 2, 2, 5, null );
            
            tbsSupport.getBlobStore().refreshEnvironmentNow();
            dbSupport.getDataManager().updateBeans(
                    CollectionFactory.toSet( TapePartition.QUIESCED ),
                    BeanFactory.newBean( TapePartition.class ).setQuiesced( Quiesced.NO ), 
                    Require.nothing() );
            
            RpcClientInvocationDiagnostics.getInstance().enable();
            int c = 0, i = 0; // 100ms X 100 = 10000ms = 10s => Yes, intentional.
            do
            {
                tbsSupport.mockPeriodicStartNewTasksCall();
                TestUtil.sleep( 100 );
                c = tapeService.getCount( Require.beanPropertyEquals( 
                                            Tape.STATE, TapeState.NORMAL ) );
                Logger.getLogger( this.getClass() ).info(
                    "Tapes set to NORMAL by end of iteraton " + i + ": " + c );
                
            } while ( 100 > ++i && 10 != c ) ;
            
            assertEquals(
                    10,
                    tapeService.getCount( Require.beanPropertyEquals(
                            Tape.STATE, TapeState.NORMAL ) ),
                    "All tapes shoulda switched to normal state."
                    );
            
            final List< TapeDrive > drives = tapeDriveService.retrieveAll().toList();
            final BasicTestsInvocationHandler btih1 = RpcClientInvocationDiagnostics.getInstance().getBtih(
                    TapeDriveResource.class, 
                    drives.get( 0 ).getSerialNumber() );
            final BasicTestsInvocationHandler btih2 = RpcClientInvocationDiagnostics.getInstance().getBtih(
                    TapeDriveResource.class, 
                    drives.get( 1 ).getSerialNumber() );
            final BasicTestsInvocationHandler btih3 = RpcClientInvocationDiagnostics.getInstance().getBtih(
                    TapeDriveResource.class, 
                    drives.get( 2 ).getSerialNumber() );
            final BasicTestsInvocationHandler btih4 = RpcClientInvocationDiagnostics.getInstance().getBtih(
                    TapeDriveResource.class, 
                    drives.get( 3 ).getSerialNumber() );
            final Method methodFormat = ReflectUtil.getMethod( TapeDriveResource.class, "format" );
            assertEquals(
                    10,
                    btih1.getMethodCallCount( methodFormat ) + btih2.getMethodCallCount( methodFormat )
                            + btih3.getMethodCallCount( methodFormat ) + btih4.getMethodCallCount( methodFormat ),
                    "Each of the tapes shoulda been formatted."
                     );
        }
        finally
        {
            simulator.shutdown();
            tbsSupport.shutdown();
        }
    }

    @Test
    public void testTasksCreatedDs3BlobStore() {
        try {
            final DataPlannerIntegrationTester tester = new DataPlannerIntegrationTester(Simulator.getTestConfig());
            final UUID adminId = tester.getAdminUser().getId();
        } catch(Exception e ) {

        }

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
