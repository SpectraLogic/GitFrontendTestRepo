/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator;

import com.spectralogic.s3.common.platform.lang.RuntimeInformationLogger;
import com.spectralogic.s3.common.rpc.RpcServerPort;
import com.spectralogic.s3.simulator.lang.PoolConfig;
import com.spectralogic.s3.simulator.lang.SimulatorConfig;
import com.spectralogic.s3.simulator.poolresource.SimPoolEnvironmentResource;
import com.spectralogic.s3.simulator.poolresource.SimPoolManagerImpl;
import com.spectralogic.s3.simulator.poolresource.SimPoolStateManager;
import com.spectralogic.s3.simulator.state.SimStateManager;
import com.spectralogic.s3.simulator.state.simresource.SimStateManagerImpl;
import com.spectralogic.util.healthmon.*;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.log.LogUtil;
import com.spectralogic.util.marshal.JsonMarshaler;
import com.spectralogic.util.net.rpc.server.RpcServer;
import com.spectralogic.util.net.rpc.server.RpcServerImpl;
import com.spectralogic.util.shutdown.BaseShutdownable;
import com.spectralogic.util.shutdown.StandardShutdownListener;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

/**
 * Simulates an empty pool backend.  May be brought up in its own process or as part of an already-running process.
 * <br><br>
 */
public final class PoolSimulator extends BaseShutdownable implements Runnable
{
    /**
     * Use this mechanism to bring up a simulator as its own process (for example, if starting the simulator
     * as a service like the data planner service).
     */
    public static void main( final String[] args )
    {
        s_runInDedicatedProcess = true;
        logToStandardOut( "Pool Simulator starting up..." );

        new PoolSimulator().run();

    }


    /**
     * Use this mechanism to bring up a simulator as part of an already-running process (usually the process
     * of a JUnit test runner so that the test can run against a simulated backend).
     */
    public PoolSimulator()
    {
        // empty
    }
    
    
    private static void logToStandardOut( final String message )
    {
        final PrintStream ps = System.out;
        ps.println( new Date().toString() + ": " + message );
    }


    public void run()
    {
        loadConfig();
        LOG.info( Platform.NEWLINE + Platform.NEWLINE + 
                            "Logging Pool Simulator's basic runtime information." );
        RuntimeInformationLogger.logRuntimeInformation();
        
        LOG.info( Platform.NEWLINE + Platform.NEWLINE +
                                       "Starting Pool Simulator health monitors." );
        startHealthMonitoring();
        
        LOG.info( Platform.NEWLINE + Platform.NEWLINE +
                                  "Starting Pool Simulator's backing RPC server." );
        final RpcServer rpcServer = new RpcServerImpl( RpcServerPort.POOL_BACKEND );
        addShutdownListener( rpcServer );
        
        LOG.warn( LogUtil.getLogMessageImportantHeaderBlock( "Pool Simulator Starting Up..." ) );
        final SimPoolStateManager stateManager = new SimPoolManagerImpl( rpcServer, m_config );
        m_stateManager = stateManager;
        rpcServer.register( null, m_stateManager );
        final SimPoolEnvironmentResource simPoolEnvResources =
                new SimPoolEnvironmentResource(m_stateManager, rpcServer);

        rpcServer.register( null, simPoolEnvResources );
        
        addShutdownListener( new StandardShutdownListener()
        {
            public void shutdownOccurred()
            {
                m_shutdownLatch.countDown();
                logToStandardOut( "Pool Simulator shutdown." );
                LOG.warn( LogUtil.getLogMessageImportantHeaderBlock( "Pool Simulator Shutdown" ) );
            }
        });
        
        try
        {
            LOG.warn( LogUtil.getLogMessageImportantHeaderBlock( "Pool Simulator Ready" ) );
            logToStandardOut( "Pool Simulator ready." );
            m_stateManagerReadyLatch.countDown();
            m_shutdownLatch.await();
        }
        catch ( final InterruptedException ex )
        {
            LOG.info( "Pool Simulator main thread has been interrupted.", ex );
        }
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

    private void loadConfig() {
        if (m_config == null) {
            String configPath = "/etc/spectra/pool_config.json";
            LOG.info("Checking for config file at: " + configPath);
            try {
                final File f = new File(configPath);
                String content = new String(Files.readAllBytes(Paths.get(f.getAbsolutePath())));
                m_config = JsonMarshaler.unmarshal(PoolConfig.class, content);
                validatePoolConfig(m_config);
            } catch (Exception e) {
                LOG.warn("Could not load config file. Creating file with defaults.", e);
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
    }

    private void validatePoolConfig( final PoolConfig config )
    {
        if ( null == config )
        {
            throw new IllegalArgumentException( "PoolConfig cannot be null." );
        }

    }

    private PoolConfig m_config = null;
    private final CountDownLatch m_stateManagerReadyLatch = new CountDownLatch( 1 );
    private final CountDownLatch m_shutdownLatch = new CountDownLatch( 1 );
    private volatile SimPoolStateManager m_stateManager;
    private static boolean s_runInDedicatedProcess = false;
    private final static Logger LOG = Logger.getLogger( PoolSimulator.class );
}
