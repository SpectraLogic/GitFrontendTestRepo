package com.spectralogic.s3.simulator.lang;

import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.util.bean.lang.DefaultBooleanValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface SimulatorConfig extends SimpleBeanSafeToProxy {

    String NUM_PARTITIONS = "numPartitions";

    int getNumPartitions();

    SimulatorConfig setNumPartitions(final int value);


    String DRIVES_PER_PARTITION = "drivesPerPartition";

    int getDrivesPerPartition();

    SimulatorConfig setDrivesPerPartition(final int value);

    String TAPES_PER_PARTITION = "tapesPerPartition";

    int getTapesPerPartition();

    SimulatorConfig setTapesPerPartition(final int value);

    String TAPE_TYPE = "tapeType";

    TapeType getTapeType();

    SimulatorConfig setTapeType(final TapeType value);

    String DRIVE_TYPE = "driveType";

    TapeDriveType getDriveType();

    SimulatorConfig setDriveType(final TapeDriveType value);

    String MAX_BYTES_PER_SECOND = "maxBytesPerSecond";

    @Optional
    Long getMaxBytesPerSecond();

    SimulatorConfig setMaxBytesPerSecond(final Long value);


    String VIRTUAL_LIBRARY_PATH = "virtualLibraryPath";

    String getVirtualLibraryPath();

    SimulatorConfig setVirtualLibraryPath(final String value);


    String UNLOAD_DELAY = "unloadDelay";

    int getUnloadDelay();

    SimulatorConfig setUnloadDelay(final int value);


    String LOAD_DELAY = "loadDelay";

    int getLoadDelay();

    SimulatorConfig setLoadDelay(final int value);


    String LOCATE_DELAY = "locateDelay";

    int getLocateDelay();

    SimulatorConfig setLocateDelay(final int value);


    String MOVE_DELAY = "moveDelay";

    int getMoveDelay();

    SimulatorConfig setMoveDelay(final int value);


    String GET_TAPE_ENVIRONMENT_DELAY = "getTapeEnvironmentDelay";

    int getGetTapeEnvironmentDelay();

    SimulatorConfig setGetTapeEnvironmentDelay(final int value);


    String FORMAT_DELAY = "formatDelay";

    int getFormatDelay();

    SimulatorConfig setFormatDelay(final int value);


    String INSPECT_DELAY = "inspectDelay";

    int getInspectDelay();

    SimulatorConfig setInspectDelay(final int value);


    String GET_TAPE_GENERATION_NUMBER_DELAY = "getTapeGenerationNumberDelay";

    int getGetTapeGenerationNumberDelay();

    SimulatorConfig setGetTapeGenerationNumberDelay(final int value);


    String DELETE_TAPES_ON_STARTUP = "deleteTapesOnStartup";

    @DefaultBooleanValue( false )
    boolean getDeleteTapesOnStartup();

    SimulatorConfig setDeleteTapesOnStartup(final boolean value);
}
