/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.spectralogic.s3.simulator.lang.SimulatorConfig;
import com.spectralogic.s3.simulator.state.simresource.SimDevices;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.marshal.JsonMarshaler;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import org.apache.log4j.Logger;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.platform.lang.RuntimeInformationLogger;
import com.spectralogic.s3.common.rpc.RpcServerPort;
import com.spectralogic.s3.simulator.state.SimStateManager;
import com.spectralogic.s3.simulator.state.simresource.SimStateManagerImpl;
import com.spectralogic.s3.simulator.state.simresource.api.SimulatorResource;
import com.spectralogic.s3.simulator.taperesource.SimTapeEnvironmentResource;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.healthmon.CpuHogDetector;
import com.spectralogic.util.healthmon.CpuHogListenerImpl;
import com.spectralogic.util.healthmon.DeadlockDetector;
import com.spectralogic.util.healthmon.DeadlockListenerImpl;
import com.spectralogic.util.healthmon.MemoryHogDetector;
import com.spectralogic.util.healthmon.MemoryHogListenerImpl;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.server.RpcServer;
import com.spectralogic.util.net.rpc.server.RpcServerImpl;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.shutdown.StandardShutdownListener;

/**
 * Simulates a tape backend.  May be brought up in its own process or as part of an already-running process.
 * <br><br>
 * 
 * The simulator is designed to provide the same kind of unpredictable, non-deterministic behavior that a
 * real backend will provide in that it will simulate delays and random tape environment change events, 
 * amongst other things.  It will also have zero tolerance for bad client use, throwing exceptions whenever
 * a client doesn't use the API in a strictly correct manner so that bugs in the client using the simulator
 * are exposed rather than masked. <br><br>
 * 
 * The only part of the simulator that is "kind" in that it won't hold you to an absolutely strict contract
 * is the {@link SimulatorResource}, which is the {@link RpcResource} used to configure the simulator itself.
 */
public final class Simulator extends BaseShutdownable implements Runnable
{
    /**
     * Use this mechanism to bring up a simulator as its own process (for example, if starting the simulator
     * as a service like the data planner service).
     */
    public static void main( final String[] args )
    {
        s_runInDedicatedProcess = true;
        logToStandardOut( "Simulator starting up..." );
        final ConfigurableApplicationContext context =
                new ClassPathXmlApplicationContext( "simulatorbeans.xml" );
        context.registerShutdownHook();

        Thread poolSimulatorThread = new Thread(new Runnable() {
            @Override
            public void run() {
                new PoolSimulator().run();
                try
                {
                    ( (PoolSimulator)context.getBean( PoolSimulator.class.getSimpleName() ) ).run();

                }
                finally
                {
                    context.close();
                }
            }
        });

        Thread simulatorThread = new Thread(new Runnable() {
            @Override
            public void run() {
                new Simulator().run();
                try
                {
                    ( (Simulator)context.getBean( Simulator.class.getSimpleName() ) ).run();
                }
                finally
                {
                    context.close();
                }
            }
        });

        poolSimulatorThread.start();
        simulatorThread.start();
    }
    
    
    /**
     * Use this mechanism to bring up a simulator as part of an already-running process (usually the process 
     * of a JUnit test runner so that the test can run against a simulated backend).
     */
    public Simulator()
    {
        // empty
    }


    public Simulator(final SimulatorConfig config)
    {
        m_config = config;
    }
    
    
    private static void logToStandardOut( final String message )
    {
        final PrintStream ps = System.out;
        ps.println( new Date().toString() + ": " + message );
    }
    
    
    /**
     * Only useful when the simulator is being run as part of an already-running process.  Use the 
     * {@link SimulatorResource} to accomplish the same thing when running the simulator as its own process.
     * @param giveUpAfterSeconds Throws RuntimeExcepton if the Simulator's state
     * manager does not become available in less than giveUpAfterSeconds seconds.
     */
    public SimStateManager getStateManager( int giveUpAfterSeconds )
    {
        try
        {
            if ( ! m_stateManagerReadyLatch.await( giveUpAfterSeconds,
                                                   TimeUnit.SECONDS ) )
            {
                if ( null == m_stateManager )
                {
                    throw new RuntimeException(
                    "Timed out waiting for Simulator state manager init to finish." );
                }
            }
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        return m_stateManager;
    }

    public static SimulatorConfig getPerfConfig() {
        final SimulatorConfig config = BeanFactory.newBean(SimulatorConfig.class);
        config.setDriveType(TapeDriveType.LTO9);
        config.setTapeType(TapeType.LTO9);
        config.setNumPartitions(1);
        config.setDrivesPerPartition(48);
        config.setTapesPerPartition(1000);
        config.setMaxBytesPerSecond(400L * 1024 * 1024);
        config.setVirtualLibraryPath("/etc/spectra/simulator_data");
        config.setLocateDelay(45 * 1000);
        config.setLoadDelay(17 * 1000);
        config.setUnloadDelay(195 * 1000);
        config.setMoveDelay(45 * 1000);
        return config;
    }


    public static SimulatorConfig getTestConfig() {
        final SimulatorConfig config = BeanFactory.newBean(SimulatorConfig.class);
        config.setDriveType(TapeDriveType.LTO6);
        config.setTapeType(TapeType.LTO6);
        config.setNumPartitions(1);
        config.setDrivesPerPartition(1);
        config.setTapesPerPartition(1);
        config.setMaxBytesPerSecond(null);
        config.setVirtualLibraryPath(System.getProperty("java.io.tmpdir") + Platform.FILE_SEPARATOR + "simulator_data");
        config.setLocateDelay(0);
        config.setLoadDelay(0);
        config.setUnloadDelay(0);
        config.setMoveDelay(0);
        config.setDeleteTapesOnStartup(true);
        return config;
    }


    public static SimulatorConfig getEmptyConfig() {
        final SimulatorConfig config = BeanFactory.newBean(SimulatorConfig.class);
        config.setDriveType(TapeDriveType.LTO6);
        config.setTapeType(TapeType.LTO6);
        config.setDrivesPerPartition(0);
        config.setTapesPerPartition(0);
        config.setMaxBytesPerSecond(null);
        config.setVirtualLibraryPath(System.getProperty("java.io.tmpdir") + Platform.FILE_SEPARATOR + "simulator_data");
        config.setLocateDelay(0);
        config.setLoadDelay(0);
        config.setUnloadDelay(0);
        config.setMoveDelay(0);
        return config;
    }
    
    public void run()
    {
        if (m_config == null) {
            String configPath = "/etc/spectra/simulator_config.json";
            LOG.info("Checking for config file at: " + configPath);
            try {
                final File f = new File(configPath);
                String content = new String(Files.readAllBytes(Paths.get(f.getAbsolutePath())));
                m_config = JsonMarshaler.unmarshal(SimulatorConfig.class, content);
                validateSimulatorConfig(m_config);
            } catch (Exception e) {
                LOG.warn("Could not load config file. Creating file with defaults.", e);
                m_config = getPerfConfig();
                final String s = JsonMarshaler.formatPretty(
                        JsonMarshaler.marshal(m_config, NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE));
                try {
                    final Path path = Paths.get(configPath);
                    Files.deleteIfExists(path);
                    Files.createDirectories(path.getParent());
                    Files.write(path, s.getBytes());
                } catch (IOException ex) {
                    LOG.warn("Could not save default config file to " + configPath, ex);
                }
            }
        }
        if ( !m_config.getVirtualLibraryPath().endsWith( Platform.FILE_SEPARATOR ) )
        {
            m_config.setVirtualLibraryPath( m_config.getVirtualLibraryPath() + Platform.FILE_SEPARATOR );
        }

        //if delete on startup is true, delete and recreate the library directory:
        if (m_config.getDeleteTapesOnStartup()) {
            LOG.info("Delete tapes on startup enabled in config. Will attempt to delete all files in virtual library directory: " + m_config.getVirtualLibraryPath());
            try {
                final File f = new File(m_config.getVirtualLibraryPath());
                if (f.exists()) {
                    for (File file : f.listFiles()) {
                        file.delete();
                    }
                }
            } catch (Exception e) {
                LOG.warn("Could not delete files in library directory: " + m_config.getVirtualLibraryPath(), e);
            }
            LOG.info("Deleted all files in virtual library directory.");
        }

        LOG.info( Platform.NEWLINE + Platform.NEWLINE + 
                            "Logging Simulator's basic runtime information." );
        RuntimeInformationLogger.logRuntimeInformation();
        
        LOG.info( Platform.NEWLINE + Platform.NEWLINE +
                                       "Starting Simulator health monitors." );
        startHealthMonitoring();
        
        LOG.info( Platform.NEWLINE + Platform.NEWLINE +
                                  "Starting Simulator's backing RPC server." );
        final RpcServer rpcServer = new RpcServerImpl( RpcServerPort.TAPE_BACKEND );
        addShutdownListener( rpcServer );
        
        LOG.warn( LogUtil.getLogMessageImportantHeaderBlock( "Simulator Starting Up..." ) );
        
        final SimStateManager stateManager = new SimStateManagerImpl( rpcServer, m_config );
        m_stateManager = stateManager;
        rpcServer.register( null, m_stateManager );

        final SimTapeEnvironmentResource mediaChangerResource = 
                new SimTapeEnvironmentResource( m_stateManager, rpcServer );
        rpcServer.register( 
                null,
                mediaChangerResource );
        
        addShutdownListener( new StandardShutdownListener()
        {
            public void shutdownOccurred()
            {
                m_shutdownLatch.countDown();
                logToStandardOut( "Simulator shutdown." );
                LOG.warn( LogUtil.getLogMessageImportantHeaderBlock( "Simulator Shutdown" ) );
            }
        });
        //Start Javalin REST Server
        final int restPort = 7070;
        app = Javalin.create(config -> {
            config.showJavalinBanner = false;
            config.requestLogger.http((ctx, ms) -> {
                LOG.info("REST Request: " + ctx.method() + " " + ctx.path() + ctx.method() + "Status: " + ctx.status() + " - Time: " + ms + "ms" );
            });
        }).start(restPort);

        logToStandardOut("Simulator REST interface started. Access at http://localhost:" + restPort + "/simulator");
        LOG.info("Simulator REST interface started on port {}"+ restPort);

        // Define REST Endpoints using the new SimulatorRoutes class <<-- MODIFIED PART
        SimulatorRoutes routes = new SimulatorRoutes( app, stateManager, mediaChangerResource);

        routes.registerEndpoints();
        try
        {
            LOG.warn( LogUtil.getLogMessageImportantHeaderBlock( "Simulator Ready" ) );
            logToStandardOut( "Simulator ready." );
            m_stateManagerReadyLatch.countDown();
            m_shutdownLatch.await();
        }
        catch ( final InterruptedException ex )
        {
            LOG.info( "Simulator main thread has been interrupted.", ex );
        }

        // Register the endpoints

        //Add Global Exception Handling for Javalin
        app.exception(Exception.class, (e, ctx) -> {
            LOG.error("Unhandled exception caught by Javalin request handler for path: "+ ctx.path() + e);
            ctx.status(HttpStatus.INTERNAL_SERVER_ERROR);
            ctx.json(Map.of(
                    "error", "An unexpected internal server error occurred.",
                    "message", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName(),
                    "path", ctx.path()
            ));
        });

        //Enhance Shutdown Hook (Optional but Recommended)
        final Javalin finalApp = app;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logToStandardOut("Shutdown hook: Stopping Javalin server...");
            LOG.info("Shutdown hook: Stopping Javalin server...");
            try {
                finalApp.stop();
            } catch (Exception e) {
                LOG.error("Error stopping Javalin server during shutdown", e);
            }
            logToStandardOut("Shutdown hook: Javalin server stopped.");
            LOG.info("Shutdown hook: Javalin server stopped.");
        }, "JavalinShutdownThread"));
    }

    
    private void startHealthMonitoring()
    {
        new DeadlockDetector( 30000 ).addDeadlockListener( 
                new DeadlockListenerImpl() );
        new MemoryHogDetector( 30000 ).addMemoryHogListener(
                new MemoryHogListenerImpl( 0.6f, 0.8f ) );
        new CpuHogDetector( 5000, 4000 ).addCpuHogListener( 
                new CpuHogListenerImpl() );
    }


    private void validateSimulatorConfig( final SimulatorConfig config )
    {
        if ( null == config )
        {
            throw new IllegalArgumentException( "SimulatorConfig cannot be null." );
        }

        if ( null == config.getDriveType() )
        {
            throw new IllegalArgumentException( "DriveType cannot be null." );
        }

        if ( null == config.getTapeType() )
        {
            throw new IllegalArgumentException( "TapeType cannot be null." );
        }

        if ( config.getDrivesPerPartition() < 0 )
        {
            throw new IllegalArgumentException( "Number of drives must be at least 0." );
        }

        if ( config.getTapesPerPartition() < 0 )
        {
            throw new IllegalArgumentException( "Number of tapes must be at least 0." );
        }

        if ( null == config.getVirtualLibraryPath() )
        {
            throw new IllegalArgumentException( "Virtual library path cannot be null." );
        }

        if ( config.getLocateDelay() < 0 )
        {
            throw new IllegalArgumentException( "Locate delay must be at least 0." );
        }

        if ( config.getLoadDelay() < 0 )
        {
            throw new IllegalArgumentException( "Load delay must be at least 0." );
        }

        if ( config.getUnloadDelay() < 0 )
        {
            throw new IllegalArgumentException( "Unload delay must be at least 0." );
        }

        if ( config.getMoveDelay() < 0 )
        {
            throw new IllegalArgumentException( "Move delay must be at least 0." );
        }

        if ( null != config.getMaxBytesPerSecond() && config.getMaxBytesPerSecond() < 1 )
        {
            throw new IllegalArgumentException( "Max bytes per second must be at least 1." );
        }

    }
    
    private SimulatorConfig m_config = null;
    private volatile SimStateManager m_stateManager;
    private final CountDownLatch m_stateManagerReadyLatch = new CountDownLatch( 1 );
    private final CountDownLatch m_shutdownLatch = new CountDownLatch( 1 );

    private static boolean s_runInDedicatedProcess = false;
    public final static String DEFAULT_LIBRARY_SN = "ALL_TAPE_PARTITIONS";
    public final static String DEFAULT_PARTITION_SN = "defaultsimpartition";
    public final static String DEFAULT_DRIVE_SN = "defaultsimdrive";
    public final static String DEFAULT_TAPE_SN = "defaultsimtape";
    public final static String DEFAULT_POOL_SN = "defaultpooltape";
    private final static Logger LOG = Logger.getLogger( Simulator.class );
    static Javalin app = null;
}
