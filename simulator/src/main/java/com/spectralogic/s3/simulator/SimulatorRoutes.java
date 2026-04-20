package com.spectralogic.s3.simulator;


import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.rpc.tape.TapeEnvironmentResource;
import com.spectralogic.s3.simulator.lang.SimulatorConfig;
import com.spectralogic.s3.simulator.poolresource.SimPoolEnvironmentResource;
import com.spectralogic.s3.simulator.poolresource.SimPoolStateManager;
import com.spectralogic.s3.simulator.state.SimStateManager;
import com.spectralogic.s3.simulator.state.simresource.SimDevices;
import com.spectralogic.s3.simulator.taperesource.ErrorSimTapeEnvironmentResource;
import com.spectralogic.s3.simulator.taperesource.SimTapeEnvironmentResource;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Platform;
import io.javalin.Javalin;

import org.apache.log4j.Logger;




/**
 * Defines the REST API endpoints for the Simulator.
 */
public class SimulatorRoutes {

    private static final Logger LOG = Logger.getLogger(SimulatorRoutes.class);

    private final Javalin app;// Reference to the main simulator logic
    private final SimStateManager stateManager;
    private final SimPoolStateManager poolStateManager;
    private final SimTapeEnvironmentResource mediaChangerResource;
    private final SimPoolEnvironmentResource poolEnvironmentResource;
    TapeEnvironmentResource goodTapeEnvResource;
    SimulatorConfig existingConfig ;
    public SimulatorRoutes(Javalin app, SimStateManager stateManager,SimTapeEnvironmentResource mediaChangerResource ) {
        this.app = app;
        this.stateManager = stateManager;
        this.mediaChangerResource = mediaChangerResource;
        this.poolEnvironmentResource = null;
        this.poolStateManager = null;
    }

    public SimulatorRoutes(Javalin app, SimPoolStateManager poolStateManager, SimPoolEnvironmentResource mediaChangerResource ) {

        this.app = app;
        this.stateManager = null;
        this.mediaChangerResource = null;
        this.poolEnvironmentResource = mediaChangerResource;
        this.poolStateManager = poolStateManager;
    }

    /**
     * Registers all simulator REST endpoints with the Javalin app.
     */
    public void registerEndpoints() {

        app.get("/simulator", ctx -> {
            ctx.result("Simulator is running!");
        });

        app.get("/swapErrorTapeEnvironment", ctx -> {
            goodTapeEnvResource = this.stateManager.swapTapeEnvironmentResource(new ErrorSimTapeEnvironmentResource());
            ctx.result("Tape environment resource swapped to error TapeEnvironmentResource !");

        });

        app.get("/swapTapeEnvironment", ctx -> {
            this.stateManager.swapTapeEnvironmentResource(goodTapeEnvResource);
            ctx.result("Tape environment resource swapped to default TapeEnvironmentResource !");
        });


        app.get("/changeSingleConfig", ctx -> {
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
            existingConfig = this.stateManager.swapConfig(config);
            ctx.result("Tape config changed to single drive/tap.!");
        });

        app.get("/restoreConfig", ctx -> {
            this.stateManager.swapConfig(existingConfig);
            ctx.result("Restored tape config.");
        });

        app.get("/cleanupFiles", ctx -> {
            if (this.poolStateManager != null) {
                this.poolStateManager.cleanup();
            } else {
                this.stateManager.cleanup();
            }

            ctx.result("Restored config.");
        });

    }
}