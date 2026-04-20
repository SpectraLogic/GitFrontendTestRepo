/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator.state;

import java.util.Map;
import java.util.Set;

import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.TapeEnvironmentResource;
import com.spectralogic.s3.common.rpc.tape.domain.ElementAddressBlockInformation;
import com.spectralogic.s3.simulator.domain.SimDrive;
import com.spectralogic.s3.simulator.domain.SimLibrary;
import com.spectralogic.s3.simulator.domain.SimPartition;
import com.spectralogic.s3.simulator.domain.SimTape;
import com.spectralogic.s3.simulator.lang.SimulatorConfig;
import com.spectralogic.s3.simulator.state.simresource.SimDevices;
import com.spectralogic.s3.simulator.state.simresource.api.SimulatorResource;

public interface SimStateManager extends SimulatorResource
{
    /**
     * @return If something's changed inside or outside the explicit RPC call
     * ultimately driving the call to this method, then return true. If
     * nothing's changed, then occasionally return true anyway to simulate
     * parallel confliciting activity.
     */
    boolean hasChanged();
    
    
    SimLibrary getLibrary( final String serialNumber );
    
    
    SimPartition getPartition( final String serialNumber );
    
    
    SimDrive getDrive( final String serialNumber );
    
    
    SimDrive getDrive( final String partitionSerialNumber, final int elementAddress );
    
    
    SimTape getTape( final String serialNumber );
    
    
    SimTape getTape( final String partitionSerialNumber, final int elementAddress );
    
    
    Set< SimLibrary > getLibraries();
    
    
    Set< SimPartition > getPartitions( final String librarySerialNumber );
    
    
    Set< SimDrive > getDrives( final String partitionSerialNumber );
    
    
    Set< SimTape > getTapes( final String partitionSerialNumber );
    
    
    Set< ElementAddressBlockInformation > getElementAddressBlocks( final String partitionSerialNumber );
    
    
    void simulateDelay( final int maxMillis );


    void simulateDelay( final int maxMillis, final String action );

    TapeEnvironmentResource swapTapeEnvironmentResource(final TapeEnvironmentResource newResource);

    TapeDriveResource swapTapeDriveResource(final String serialNumber, final TapeDriveResource resource);

    void swapErrorTapeEnvironmentResource();
    void swapGoodTapeEnvironmentResource();

    SimDevices swapSimDevices(final SimDevices newDevices);

    SimulatorConfig getSimulatorConfig();

    int getStartDriveRange();

    int getEndDriveRange();

    int getStartTapeRange();

    int getEndTapeRange();

    int getStartIERange();

    int getEndIERange();

    void cleanup();

    SimulatorConfig swapConfig(SimulatorConfig config);

    void registerTapeDriveResource(String serialNumber, TapeDriveResource resource);

    TapeDriveResource unregisterTapeDriveResource(String serialNumber);

    TapeDriveResource getTapeDriveResource(String serialNumber);

    Map<String, TapeDriveResource> getTapeDriveResources();
}
