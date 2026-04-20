package com.spectralogic.s3.dataplanner.testfrmwrk;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.notification.S3ObjectCachedNotificationRegistration;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BucketService;
import com.spectralogic.s3.common.dao.service.planner.CacheFilesystemService;
import com.spectralogic.s3.common.rpc.RpcServerPort;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.DataPolicyManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.PoolManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.common.rpc.tape.TapeEnvironmentResource;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.DataPlanner;
import com.spectralogic.s3.simulator.PoolSimulator;
import com.spectralogic.s3.simulator.Simulator;
import com.spectralogic.s3.simulator.lang.SimulatorConfig;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.net.rpc.client.RpcClient;
import com.spectralogic.util.net.rpc.client.RpcClientImpl;
import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.notification.dispatch.NotificationListener;
import com.spectralogic.util.notification.domain.Notification;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DataPlannerIntegrationTester implements AutoCloseable {

    public DataPlannerIntegrationTester() {
        this (null);
    }

    public DataPlannerIntegrationTester(final SimulatorConfig simulatorConfig) {
        initializeConfigFiles();
        m_dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        m_dbSupport.reset();
        m_dbSupport.getServiceManager().getService( BucketService.class ).initializeLogicalSizeCache();
        final BeansServiceManager serviceManager = m_dbSupport.getServiceManager();
        System.out.println("Testing with local database: " + m_dbSupport.getDbName());
        m_mockDaoDriver = new MockDaoDriver(m_dbSupport);
        m_admin = m_mockDaoDriver.createUser("Administrator");
        m_cacheFilesystemDriver = new MockCacheFilesystemDriver(m_dbSupport);
        serviceManager.getService(CacheFilesystemService.class).retrieveAll().getFirst();


        if (simulatorConfig != null) {
            m_simulator = new Simulator(simulatorConfig);
        } else {
            m_simulator = new Simulator();
        }

        m_tapeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                m_simulator.run();
            }
        });

        m_poolSimulator = new PoolSimulator();
        m_poolThread = new Thread(new Runnable() {
            @Override
            public void run() {
                m_poolSimulator.run();
            }
        });

        m_dataPlanner = new DataPlanner(m_dbSupport.getDataSource());
        m_dpThread = new Thread(new Runnable() {
            @Override
            public void run() {
                m_dataPlanner.run();
            }
        });
    }


    private void initializeConfigFiles() {
        LOG.info("Initializing config files...");
        //NOTE: the appropriate source dir depends if we are running as a unit test or with a main method
        final Path source1 = Paths.get("src/test/resources/etc/");
        final Path source2 = Paths.get("DataPlanner/src/test/resources/etc/");
        final Path source;
        //use whichever directory exists:
        if (Files.exists(source1)) {
            source = source1;
        } else if (Files.exists(source2)) {
            source = source2;
        } else {
            throw new RuntimeException("Failed to find config files in " + source1 + " or " + source2);
        }
        final Path target = Paths.get("/etc/");
        try {
            Files.walk(source).forEach(s -> {
                try {
                    final Path t = target.resolve(source.relativize(s));
                    if (!Files.exists(t)) {
                        Files.copy(s, t);
                    }
                } catch (AccessDeniedException e) {
                    throw new RuntimeException("Failed to copy config files from " + s + " to " + target + ". Access denied. Please manually copy the files from " + source.toAbsolutePath() + " to " + target.toAbsolutePath(), e);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to copy config file from " + s + " to " + target.resolve(source.relativize(s)), e);
                }
            });
        } catch (AccessDeniedException e) {
            throw new RuntimeException("Failed to copy config files from. Access denied. Please manually copy the files from " + source + " to " + target + ".", e);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to copy config files.", e);
        }
    }


    public void start() {
        LOG.info("Starting tape backend simulator...");
        m_tapeThread.start();
        final RpcClient rpcTapeClient = new RpcClientImpl("localhost", RpcServerPort.TAPE_BACKEND );
        final TapeEnvironmentResource tapeEnvResource = rpcTapeClient.getRpcResource(
                TapeEnvironmentResource.class,
                null,
                ConcurrentRequestExecutionPolicy.CONCURRENT );

        LOG.info("Starting pool backend simulator...");
        m_poolThread.start();
        final RpcClient rpcPoolClient = new RpcClientImpl("localhost", RpcServerPort.POOL_BACKEND );
        m_poolEnvResource = rpcPoolClient.getRpcResource(
                PoolEnvironmentResource.class,
                null,
                ConcurrentRequestExecutionPolicy.CONCURRENT );

        LOG.info("Starting DataPlanner...");
        m_dpThread.start();
        final RpcClient rpcClient = new RpcClientImpl("localhost", RpcServerPort.DATA_PLANNER );
        m_tapeResource = rpcClient.getRpcResource(
                TapeManagementResource.class,
                null,
                ConcurrentRequestExecutionPolicy.CONCURRENT );
        m_plannerResource = rpcClient.getRpcResource(
                DataPlannerResource.class,
                null,
                ConcurrentRequestExecutionPolicy.CONCURRENT );
        m_poolManagementResource = rpcClient.getRpcResource(
                PoolManagementResource.class,
                null,
                ConcurrentRequestExecutionPolicy.CONCURRENT );
        m_policyResource = rpcClient.getRpcResource(
                DataPolicyManagementResource.class,
                null,
                ConcurrentRequestExecutionPolicy.CONCURRENT );
    }

    @Override
    public void close() {
        m_dataPlanner.shutdown();
        m_dpThread.interrupt();
        m_simulator.shutdown();
        m_tapeThread.interrupt();
        m_poolSimulator.shutdown();
        m_poolThread.interrupt();
    }

    public BeansServiceManager getServiceManager() {
        return m_dbSupport.getServiceManager();
    }
    public DataPlanner getDataplanner() {
        return m_dataPlanner;
    }

    public Simulator getTapeBackendSimulator() {
        return m_simulator;
    }

    public MockDaoDriver getMockDaoDriver() {
        return m_mockDaoDriver;
    }

    public TapeManagementResource getTapeResource() {
        return m_tapeResource;
    }

    public PoolEnvironmentResource getPoolEnvResource() {
        return m_poolEnvResource;
    }

    public PoolManagementResource getPoolManagementResource() {
        return m_poolManagementResource;
    }

    public DataPolicyManagementResource getPolicyResource() {
        return m_policyResource;
    }

    public DataPlannerResource getPlannerResource() {
        return m_plannerResource;
    }

    public MockCacheFilesystemDriver getCacheFilesystemDriver() {
        return m_cacheFilesystemDriver;
    }

    public User getAdminUser() {
        return m_admin;
    }


    private final MockCacheFilesystemDriver m_cacheFilesystemDriver;
    private final Thread m_tapeThread;
    private final Thread m_poolThread;
    private final Thread m_dpThread;
    private final Simulator m_simulator;
    private final PoolSimulator m_poolSimulator;
    private final DataPlanner m_dataPlanner;
    private final User m_admin;
    private final DatabaseSupport m_dbSupport;
    private final MockDaoDriver m_mockDaoDriver;
    private PoolEnvironmentResource m_poolEnvResource;
    private TapeManagementResource m_tapeResource;
    private DataPolicyManagementResource m_policyResource;
    private DataPlannerResource m_plannerResource;
    private PoolManagementResource m_poolManagementResource;
    private final static Logger LOG = Logger.getLogger(DataPlannerIntegrationTester.class);

}
