/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator.state.simresource.api;

import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.rpc.tape.domain.ElementAddressBlockInformation;
import com.spectralogic.s3.simulator.domain.SimDrive;
import com.spectralogic.s3.simulator.domain.SimLibrary;
import com.spectralogic.s3.simulator.domain.SimPartition;
import com.spectralogic.s3.simulator.domain.SimTape;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceName;

@RpcResourceName( "Simulator" )
public interface SimulatorResource extends RpcResource
{
    @RpcMethodReturnType( SimLibrary.class )
    RpcFuture< SimLibrary > addLibrary( final SimLibrary library );
    
    
    @RpcMethodReturnType( SimPartition.class )
    RpcFuture< SimPartition > addPartition( final SimPartition partition );
    

    @RpcMethodReturnType( SimPartition.class )
    RpcFuture< SimPartition > updatePartition( 
            final String serialNumber, final String message, final boolean online );
    
    
    @RpcMethodReturnType( SimDrive.class )
    RpcFuture< SimDrive > addDrive( final SimDrive tapeDrive );
    
    
    @RpcMethodReturnType( SimDrive.class )
    RpcFuture< SimDrive > updateDrive( 
            final String serialNumber, final String message, final boolean online );
    

    @RpcMethodReturnType( SimTape.class )
    RpcFuture< SimTape > addTape( final SimTape tape );
    
    
    @RpcMethodReturnType( SimTape.class )
    RpcFuture< SimTape > updateTape( final String serialNumber, final boolean online );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > setElementAddressBlocks( 
            final String partitionSerialNumber,
            final ElementAddressType type,
            final ElementAddressBlockInformation [] blocksForType );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > addElementAddressBlock( 
            final String partitionSerialNumber,
            final ElementAddressType type,
            final int startAddress,
            final int endAddress );
    

    @RpcMethodReturnType( void.class )
    RpcFuture< ? > reset( 
            final int numberOfPartitions,
            final int drivesPerPartition,
            final int tapesPerPartition,
            final String partitionRootPath );
}
