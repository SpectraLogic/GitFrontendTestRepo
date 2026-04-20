/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.spectralogic.s3.common.platform.lang.ConfigurationInformationProvider;
import com.spectralogic.s3.dataplanner.backend.api.BlobStore;
import com.spectralogic.s3.dataplanner.backend.api.PersistenceType;
import org.apache.log4j.Logger;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.spectralogic.s3.common.dao.domain.notification.BucketHistoryEvent;
import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailureType;
import com.spectralogic.s3.common.dao.properties.DataPlannerProperty;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.ds3.DataPathBackendService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManagerImpl;
import com.spectralogic.s3.common.dao.service.ds3.SystemFailureService;
import com.spectralogic.s3.common.platform.cache.CacheManager;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.platform.lang.RuntimeInformationLogger;
import com.spectralogic.s3.common.rpc.RpcServerPort;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.DataPolicyManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.TargetManagementResource;
import com.spectralogic.s3.common.rpc.target.AzureConnectionFactory;
import com.spectralogic.s3.common.rpc.target.Ds3ConnectionFactory;
import com.spectralogic.s3.common.rpc.target.PublicCloudTargetImportScheduler;
import com.spectralogic.s3.common.rpc.target.S3ConnectionFactory;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreDriver;
import com.spectralogic.s3.dataplanner.backend.driver.BlobStoreDriverImpl;
import com.spectralogic.s3.dataplanner.backend.pool.PoolBlobStoreImpl;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolBlobStore;
import com.spectralogic.s3.dataplanner.backend.tape.TapeBlobStoreImpl;
import com.spectralogic.s3.dataplanner.backend.tape.api.TapeBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.AzureTargetBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.Ds3TargetBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.PeriodicTargetVerifier;
import com.spectralogic.s3.dataplanner.backend.target.S3TargetBlobStore;
import com.spectralogic.s3.dataplanner.backend.target.api.TargetBlobStore;
import com.spectralogic.s3.dataplanner.cache.CacheManagerImpl;
import com.spectralogic.s3.dataplanner.frontend.DataPlannerResourceImpl;
import com.spectralogic.s3.dataplanner.frontend.DataPolicyManagementResourceImpl;
import com.spectralogic.s3.dataplanner.frontend.DeadJobDeleter;
import com.spectralogic.s3.dataplanner.frontend.DeadJobMonitor;
import com.spectralogic.s3.dataplanner.frontend.DeadJobMonitorImpl;
import com.spectralogic.s3.dataplanner.frontend.DefaultDataPolicyGenerator;
import com.spectralogic.s3.dataplanner.frontend.JobCreatorImpl;
import com.spectralogic.s3.dataplanner.frontend.PoolManagementResourceImpl;
import com.spectralogic.s3.dataplanner.frontend.TapeManagementResourceImpl;
import com.spectralogic.s3.dataplanner.frontend.api.JobCreator;
import com.spectralogic.s3.dataplanner.frontend.driver.IomDriverImpl;
import com.spectralogic.util.db.domain.KeyValue;
import com.spectralogic.util.db.domain.service.KeyValueService;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.manager.DataSource;
import com.spectralogic.util.db.manager.DatabasePhysicalSpaceState;
import com.spectralogic.util.db.manager.postgres.PostgresDataManager;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BeansServiceManagerImpl;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.healthmon.CpuHogDetector;
import com.spectralogic.util.healthmon.CpuHogListenerImpl;
import com.spectralogic.util.healthmon.DeadlockDetector;
import com.spectralogic.util.healthmon.DeadlockListenerImpl;
import com.spectralogic.util.healthmon.MemoryHogDetector;
import com.spectralogic.util.healthmon.MemoryHogListenerImpl;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.net.rpc.client.RpcClient;
import com.spectralogic.util.net.rpc.client.RpcClientImpl;
import com.spectralogic.util.net.rpc.server.RpcServer;
import com.spectralogic.util.net.rpc.server.RpcServerImpl;
import com.spectralogic.util.notification.dispatch.NotificationEventDispatcher;
import com.spectralogic.util.notification.dispatch.bean.HttpNotificationEventDispatcher;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.shutdown.StandardShutdownListener;
import com.spectralogic.util.thread.RecurringRunnableExecutor;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;

public final class DataPlanner extends BaseShutdownable implements Runnable
{
    public static void main( final String[] args)
    {
        logToStandardOut( "Data planner starting up..." );
        final ConfigurableApplicationContext context = 
                new ClassPathXmlApplicationContext( "plannerbeans.xml" );
        context.registerShutdownHook();
        
        try
        {
            ( (DataPlanner)context.getBean( DataPlanner.class.getSimpleName() ) ).run();
        }
        catch ( Exception e )
        {
            LOG.error( "Dataplanner failed to run ", e);
            throw e;
        }
        finally
        {
            context.close();
        }
    }
    
    
    public DataPlanner( final DataSource dataSource )
    {
        Validations.verifyNotNull( "Data source", dataSource );
        m_dataSource = dataSource;
    }


    
    private static void logToStandardOut( final String message )
    {
        final PrintStream ps = System.out;
        ps.println( new Date().toString() + ": " + message );
    }
    
    
    public void run()
    {
        RuntimeInformationLogger.logRuntimeInformation();
        startHealthMonitoring();
        
        final DataManager dataManager = new PostgresDataManager( 
                Integer.MAX_VALUE,
                CollectionFactory.< Class< ? > >toSet( DaoDomainsSeed.class ) );
        addShutdownListener( dataManager );
        dataManager.setDataSource( m_dataSource );
        final RpcClient poolRpcClient = createPoolRpcClient();
        addShutdownListener( poolRpcClient );
        final RpcClient tapeRpcClient = createTapeRpcClient();
        addShutdownListener( tapeRpcClient );
        final RpcServer rpcServer = new RpcServerImpl( RpcServerPort.DATA_PLANNER );
        addShutdownListener( rpcServer );

        final WorkPool notificationEventDispatcherWp = 
                WorkPoolFactory.createWorkPool( 4, NotificationEventDispatcher.class.getSimpleName() );
        final NotificationEventDispatcher notificationEventDispatcher =
                new HttpNotificationEventDispatcher( notificationEventDispatcherWp );
        final BeansServiceManager serviceManager = BeansServiceManagerImpl.create(
                notificationEventDispatcher,
                dataManager,
                CollectionFactory.< Class< ? > >toSet( DaoServicesSeed.class ) );
        m_serviceManager = serviceManager;
        
        LOG.info( "This data planner is the canonical authority." );
        LOG.warn( LogUtil.getLogMessageImportantHeaderBlock( "Data Planner Starting Up..." ) );
        
        serviceManager.getService( DataPathBackendService.class ).dataPathRestarted();
        serviceManager.getService( BucketService.class ).initializeLogicalSizeCache();
        
        final KeyValueService keyValueService = serviceManager.getService( KeyValueService.class );

        /*
         * We get the connection factory instances reflectively in this 
         * manner so that developers may take the target component out of their IDE class path to eliminate a 
         * plethora of namespace pollutions (e.g. the SDK names its models identical to that of the data path
         * front end).  Jason Stevens 3/16/16.
         */
        final Ds3ConnectionFactory ds3ConnectionFactory;
        final AzureConnectionFactory azureConnectionFactory;
        final S3ConnectionFactory s3ConnectionFactory;
        try
        {
            final Class< ? > ds3FactoryType = 
                    Class.forName( "com.spectralogic.s3.target.ds3target.DefaultDs3ConnectionFactory" );
            final Constructor< ? > ds3FactoryConstructor = ds3FactoryType.getConstructor(
                    BeansServiceManager.class );
            ds3ConnectionFactory = 
                    (Ds3ConnectionFactory)ds3FactoryConstructor.newInstance( serviceManager );
            
            final Class< ? > azureFactoryType = 
                    Class.forName( "com.spectralogic.s3.target.azuretarget.DefaultAzureConnectionFactory" );
            final Constructor< ? > azureFactoryConstructor = azureFactoryType.getConstructor(
                    BeansServiceManager.class );
            azureConnectionFactory = 
                    (AzureConnectionFactory)azureFactoryConstructor.newInstance( serviceManager );
            
            final Class< ? > s3FactoryType = 
                    Class.forName( "com.spectralogic.s3.target.s3target.DefaultS3ConnectionFactory" );
            final Constructor< ? > s3FactoryConstructor = s3FactoryType.getConstructor(
                    BeansServiceManager.class );
            s3ConnectionFactory = 
                    (S3ConnectionFactory)s3FactoryConstructor.newInstance( serviceManager );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( 
                    "Failed to create " + Ds3ConnectionFactory.class.getSimpleName() + ".", ex );
        }
        
        final DeadJobMonitor deadJobMonitor = new DeadJobMonitorImpl(
                1000,
                1000 * 60 * 60 * 24, // 24 hours
                serviceManager,
                ds3ConnectionFactory );
        final JobProgressManager jobProgressManager =
                new JobProgressManagerImpl( serviceManager );
        
        final CacheManager cacheManager = new CacheManagerImpl( serviceManager );
        final KeyValue kvPreferredChunkSize = keyValueService.retrieve( 
                DataPlannerProperty.PREFERRED_JOB_CHUNK_SIZE_IN_MB.toString() );
        final PoolBlobStore poolBlobStore = new PoolBlobStoreImpl(
                poolRpcClient, cacheManager, jobProgressManager, serviceManager );
        final DiskManager diskManager = poolBlobStore.getDiskManager();
        
        final TapeBlobStore tapeBlobStore = new TapeBlobStoreImpl(
                tapeRpcClient, diskManager, jobProgressManager, serviceManager );
        final TargetBlobStore ds3TargetBlobStore = new Ds3TargetBlobStore(
                ds3ConnectionFactory,
                diskManager,
                jobProgressManager,
                serviceManager );
        final TargetBlobStore azureTargetBlobStore = new AzureTargetBlobStore(
                azureConnectionFactory,
                diskManager,
                jobProgressManager,
                serviceManager );
        final TargetBlobStore s3TargetBlobStore = new S3TargetBlobStore(
                s3ConnectionFactory,
                diskManager,
                jobProgressManager,
                serviceManager );

        final Map<PersistenceType, BlobStore> blobStoresByPersistenceType = Map.of(
                PersistenceType.S3, s3TargetBlobStore,
                PersistenceType.DS3, ds3TargetBlobStore,
                PersistenceType.AZURE, azureTargetBlobStore,
                PersistenceType.POOL, poolBlobStore,
                PersistenceType.TAPE, tapeBlobStore );
        final JobCreator jobCreator = new JobCreatorImpl(
                diskManager,
                serviceManager,
                ds3ConnectionFactory,
                jobProgressManager,
                blobStoresByPersistenceType,
                64 * 1024,
                ( null == kvPreferredChunkSize ) ? null : kvPreferredChunkSize.getLongValue() );
        
        final DataPlannerResource dataPlannerResource = new DataPlannerResourceImpl(
                deadJobMonitor, 
                rpcServer, 
                serviceManager,
                diskManager, 
                jobCreator, 
                jobProgressManager, 
                tapeBlobStore,
                poolBlobStore,
                ds3ConnectionFactory )
            .addTargetBlobStore( ds3TargetBlobStore )
            .addTargetBlobStore( azureTargetBlobStore )
            .addTargetBlobStore( s3TargetBlobStore );
        new TapeManagementResourceImpl( 
                rpcServer, 
                tapeBlobStore,
                serviceManager );
        new PoolManagementResourceImpl(
                rpcServer, 
                poolBlobStore,
                serviceManager );
                
        final DataPolicyManagementResource dataPolicyManagementResource =
                new DataPolicyManagementResourceImpl( 
                        rpcServer, 
                        serviceManager );

        /*
         * We get the target management resource instance reflectively in this 
         * manner so that developers may take the target component out of their IDE class path to eliminate a 
         * plethora of namespace pollutions (e.g. the SDK names its models identical to that of the data path
         * front end).  Jason Stevens 3/16/16.
         */
        final TargetManagementResource targetManagementResource;
        try
        {
            final Class< ? > resourceType = 
                    Class.forName( "com.spectralogic.s3.target.TargetManagementResourceImpl" );
            final Constructor< ? > resourceConstructor = resourceType.getConstructor(
                    RpcServer.class, 
                    Ds3ConnectionFactory.class,
                    AzureConnectionFactory.class,
                    S3ConnectionFactory.class,
                    DataPlannerResource.class,
                    DataPolicyManagementResource.class, 
                    PublicCloudTargetImportScheduler.class,
                    PublicCloudTargetImportScheduler.class,
                    BeansServiceManager.class );
            targetManagementResource = (TargetManagementResource)resourceConstructor.newInstance(
                    rpcServer, 
                    ds3ConnectionFactory,
                    azureConnectionFactory,
                    s3ConnectionFactory,
                    dataPlannerResource,
                    dataPolicyManagementResource,
                    azureTargetBlobStore,
                    s3TargetBlobStore,
                    serviceManager );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( 
                    "Failed to create " + TargetManagementResource.class.getSimpleName() + ".", ex );
        }
        final DeadJobDeleter deadJobDeleter;
        try
        {
            deadJobDeleter = new DeadJobDeleter( 
                    1000 * 60 * 30, // 30 mins
                    serviceManager, 
                    deadJobMonitor,
                    targetManagementResource );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( 
                    "Failed to create " + DeadJobDeleter.class.getSimpleName() + ".", ex );
        }
        int blobStoreInterval = 60 * 1000;
        int iomDriverInterval = 2 * 60 * 1000;
        final String version =
                ConfigurationInformationProvider.getInstance().getBuildInformation().getVersion();
        if (version.equals("DOCKER_VERSION"))
        {
            blobStoreInterval = 1000;
            iomDriverInterval = 1000;
        }

        final BlobStoreDriver blobStoreDriver = new BlobStoreDriverImpl(
                tapeBlobStore, 
                poolBlobStore,
                ds3TargetBlobStore,
                azureTargetBlobStore,
                s3TargetBlobStore,
                serviceManager,
                diskManager,
                jobCreator,
                jobProgressManager,
                ds3ConnectionFactory,
                blobStoreInterval );
        final IomDriverImpl iomDriver = new IomDriverImpl(
        		serviceManager,
        		dataPlannerResource,
        		diskManager,
        		jobProgressManager,
        		ds3ConnectionFactory,
                iomDriverInterval);
				        
        final RecurringRunnableExecutor healthCheckerExecutor = new RecurringRunnableExecutor(
                new DataPlannerHealthChecker( serviceManager ), 1000 * 60 * 5 );
        addShutdownListener( healthCheckerExecutor );
        healthCheckerExecutor.start();
        
        // history table pruner. Set run interval to 5 minutes.
        final RecurringRunnableExecutor historyTablePrunerExecutor = new RecurringRunnableExecutor(
                new DataPlannerHistoryTablePruner( serviceManager ), 1000 * 60 * 5 );
        addShutdownListener( historyTablePrunerExecutor );
        historyTablePrunerExecutor.start();

        addShutdownListener( new DefaultDataPolicyGenerator(
                dataPolicyManagementResource,
                serviceManager,
                30000 ) );
        addShutdownListener( 
                new PeriodicTargetVerifier( serviceManager, targetManagementResource, 1000 * 3600 * 4 ) );
        
        addShutdownListener( deadJobDeleter );
        addShutdownListener( blobStoreDriver );
        addShutdownListener( iomDriver );
        addShutdownListener( new StandardShutdownListener()
        {
            public void shutdownOccurred()
            {
                logToStandardOut( "Data planner shutdown." );
                LOG.warn( LogUtil.getLogMessageImportantHeaderBlock( "Data Planner Shutdown" ) );
            }
        });
        
        try
        {
            LOG.warn( LogUtil.getLogMessageImportantHeaderBlock( "Data Planner Ready" ) );
            logToStandardOut( "Data planner ready." );
            m_waitForeverLatch.await();
        }
        catch ( final InterruptedException ex )
        {
            LOG.info( "Data planner main thread has been interrupted.", ex );
        }
    }
    
    
    private RpcClient createPoolRpcClient()
    {
        String rpcPoolClient = System.getenv("POOL_CLIENT");
        if (rpcPoolClient != null) {
            return new RpcClientImpl( rpcPoolClient, RpcServerPort.POOL_BACKEND );
        } else {
            return new RpcClientImpl( "localhost", RpcServerPort.POOL_BACKEND );
        }

    }
    
    
    private RpcClient createTapeRpcClient()
    {
        String rpcPoolClient = System.getenv("TAPE_CLIENT");
        if (rpcPoolClient != null) {
            return new RpcClientImpl( rpcPoolClient, RpcServerPort.TAPE_BACKEND );
        } else {
            return new RpcClientImpl("localhost", RpcServerPort.TAPE_BACKEND);
        }
    }
    
    
    private void startHealthMonitoring()
    {
        new DeadlockDetector( 30000 ).addDeadlockListener( 
                new DeadlockListenerImpl() );
        new MemoryHogDetector( 30000 ).addMemoryHogListener(
                new MemoryHogListenerImpl( 0.6f, 0.75f ) );
        new CpuHogDetector( 5000, 4000 ).addCpuHogListener( 
                new CpuHogListenerImpl() );
    }


    private static final class DataPlannerHealthChecker implements Runnable
    {
        private DataPlannerHealthChecker( final BeansServiceManager serviceManager )
        {
            m_serviceManager = serviceManager;
        }


        synchronized public void run()
        {
            final DatabasePhysicalSpaceState freeSpaceState = m_serviceManager.getFreeToTotalDiskSpaceRatioState();
            final DatabasePhysicalSpaceState tableSpaceState = m_serviceManager.getMaxTableToFreeRatioState();
            if ( ( freeSpaceState != DatabasePhysicalSpaceState.NORMAL ) ||
                    ( tableSpaceState != DatabasePhysicalSpaceState.NORMAL ) )
            {
                final String msg;
                if ( freeSpaceState.getFreeSpaceRatioToReachThreshold() <=
                        tableSpaceState.getFreeSpaceRatioToReachThreshold() )
                {
                    msg = "Database free space is " + freeSpaceState;
                }
                else
                {
                    msg = "Database max table size ratio to free space is " + tableSpaceState;
                }
                m_serviceManager.getService( SystemFailureService.class ).create(
                        SystemFailureType.DATABASE_RUNNING_OUT_OF_SPACE, msg +
                        ".  Please contact Spectra to increase database capacity.",
                        Integer.valueOf( 60 * 24 ) );
            }
        }
        
        private final BeansServiceManager m_serviceManager;
    } // end inner class def
    
    // inner class for monitoring history table size and pruning when necessary
    private static final class DataPlannerHistoryTablePruner implements Runnable
    {
        private DataPlannerHistoryTablePruner( final BeansServiceManager serviceManager )
        {
            m_serviceManager = serviceManager;
        }


        synchronized public void run()
        {
        	LOG.info( "History Table Pruner: checking table size" );
        	
        	// get max and min sequence number, difference should be row count, assuming
        	// the sequence numbers are contiguous
        	long maxSeqNum = m_serviceManager.getRetriever(BucketHistoryEvent.class).getMax(BucketHistoryEvent.SEQUENCE_NUMBER, Require.nothing());
        	long minSeqNum = m_serviceManager.getRetriever(BucketHistoryEvent.class).getMin(BucketHistoryEvent.SEQUENCE_NUMBER, Require.nothing());
        	long rowCount = maxSeqNum - minSeqNum;
        	if ( rowCount > HISTORY_TABLE_MAX_SIZE )
        	{
        		// delete excess rows
        		long deletePoint = maxSeqNum - HISTORY_TABLE_MAX_SIZE;
        		long numToDelete = rowCount - HISTORY_TABLE_MAX_SIZE;
        		LOG.info( "Pruning " + numToDelete + " excess rows from history table" );
        		m_serviceManager.getDeleter(BucketHistoryEvent.class).delete(
        				Require.beanPropertyLessThan( BucketHistoryEvent.SEQUENCE_NUMBER, deletePoint));
        		LOG.info( "History table pruning complete." );
        	}
        }
        
        private final static int HISTORY_TABLE_MAX_SIZE = 500000;  // yup, half a million
        private final BeansServiceManager m_serviceManager;
    } // end history table pruner inner class def
    
 
    private final CountDownLatch m_waitForeverLatch = new CountDownLatch( 1 ); 
    private final DataSource m_dataSource;
    private BeansServiceManager m_serviceManager = null;
    
    private final static Logger LOG = Logger.getLogger( DataPlanner.class );

    public  BeansServiceManager getServiceManager() {
        return m_serviceManager;
    }
}
