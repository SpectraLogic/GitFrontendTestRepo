/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator.taperesource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.ImportExportConfiguration;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.TapeEnvironmentResource;
import com.spectralogic.s3.common.rpc.tape.TapePartitionResource;
import com.spectralogic.s3.common.rpc.tape.domain.BasicTapeInformation;
import com.spectralogic.s3.common.rpc.tape.domain.ElementAddressBlockInformation;
import com.spectralogic.s3.common.rpc.tape.domain.TapeDriveInformation;
import com.spectralogic.s3.common.rpc.tape.domain.TapeEnvironmentInformation;
import com.spectralogic.s3.common.rpc.tape.domain.TapeLibraryInformation;
import com.spectralogic.s3.common.rpc.tape.domain.TapePartitionInformation;
import com.spectralogic.s3.simulator.domain.SimDrive;
import com.spectralogic.s3.simulator.domain.SimLibrary;
import com.spectralogic.s3.simulator.domain.SimPartition;
import com.spectralogic.s3.simulator.domain.SimTape;
import com.spectralogic.s3.simulator.lang.SimulatorException;
import com.spectralogic.s3.simulator.lang.SimulatorFailure;
import com.spectralogic.s3.simulator.state.SimStateManager;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseRpcResource;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.net.rpc.server.RpcServer;
import com.spectralogic.util.net.rpc.server.SerialAccessRpcResourceFactory;
import org.apache.log4j.Logger;

public final class SimTapeEnvironmentResource extends BaseRpcResource implements TapeEnvironmentResource
{
    public SimTapeEnvironmentResource(
            final SimStateManager stateManager,
            final RpcServer rpcServer )
    {
        m_stateManager = stateManager;
        m_rpcServer = rpcServer;
    }
    
    
    public RpcFuture< ? > quiesceState()
    {
        return null;
    }
    
    
    public RpcFuture< Long > getTapeEnvironmentGenerationNumber()
    {
        m_stateManager.simulateDelay( m_stateManager.getSimulatorConfig().getGetTapeGenerationNumberDelay() );
        return new RpcResponse<>( Long.valueOf( m_stateManager.hasChanged() ? 
                m_tapeEnvironmentGenerationNumber.incrementAndGet() 
                : m_tapeEnvironmentGenerationNumber.get() ) );
    }

    
    public RpcFuture< TapeEnvironmentInformation > getTapeEnvironment()
    {
        m_stateManager.simulateDelay( m_stateManager.getSimulatorConfig().getGetTapeEnvironmentDelay() );
        m_stateManager.hasChanged();
        final TapeEnvironmentInformation retval = BeanFactory.newBean( TapeEnvironmentInformation.class );
        
        final List< TapeLibraryInformation > libraries = new ArrayList<>();
        for ( final SimLibrary s : m_stateManager.getLibraries() )
        {
            final TapeLibraryInformation library =
                    BeanFactory.newBean( TapeLibraryInformation.class );
            library.setSerialNumber( s.getSerialNumber() );
            library.setName( "S/N " + s.getSerialNumber() );
            library.setManagementUrl( s.getSerialNumber() + ".tapelibrary.sldomain.local" );
            library.setPartitions( discoverPartitions( library.getSerialNumber() ) );
            if ( 0 < library.getPartitions().length )
            {
                libraries.add( library );
            }
        }
        
        retval.setLibraries( CollectionFactory.toArray( TapeLibraryInformation.class, libraries ) );
        initializeTapePartitionResources();
        initializeTapeDriveResources();
        
        return new RpcResponse<>( retval );
    }

    
    private TapePartitionInformation [] discoverPartitions( final String librarySerialNumber )
    {
        final List< TapePartitionInformation > partitions = new ArrayList<>();
        for ( final SimPartition s : m_stateManager.getPartitions( librarySerialNumber ) )
        {
            final TapePartitionInformation partition =
                    BeanFactory.newBean( TapePartitionInformation.class );
            BeanCopier.copy( partition, s );
            partition.setName( "S/N " + s.getSerialNumber() );

            partition.setDrives( discoverDrives( partition.getSerialNumber() ) );
            partition.setTapes( discoverTapes( partition.getSerialNumber() ) );
            partition.setElementAddressBlocks( discoverAddressBlocks( partition.getSerialNumber() ) );
            partition.setImportExportConfiguration( ImportExportConfiguration.SUPPORTED );
            partitions.add( partition );
        }


        return CollectionFactory.toArray( TapePartitionInformation.class, partitions );
    }
    
    
    private TapeDriveInformation [] discoverDrives( final String serialNumber )
    {
        final List< TapeDriveInformation > retval = new ArrayList<>();
        
        for ( final SimDrive s : m_stateManager.getDrives( serialNumber ) )
        {
            final TapeDriveInformation drive = BeanFactory.newBean( TapeDriveInformation.class );
            BeanCopier.copy( drive, s );
            retval.add( drive );
        }
        
        return CollectionFactory.toArray( TapeDriveInformation.class, retval );
    }
    
    
    private BasicTapeInformation [] discoverTapes( final String serialNumber )
    {
        final List< BasicTapeInformation > retval = new ArrayList<>();
        
        for ( final SimTape s : m_stateManager.getTapes( serialNumber ) )
        {
            final BasicTapeInformation tape = BeanFactory.newBean( BasicTapeInformation.class );
            BeanCopier.copy( tape, s );
            retval.add( tape );
        }
        
        return CollectionFactory.toArray( BasicTapeInformation.class, retval );
    }
    
    
    private ElementAddressBlockInformation [] discoverAddressBlocks( final String serialNumber )
    {
        final List< ElementAddressBlockInformation > retval = new ArrayList<>();
        
        for ( final ElementAddressBlockInformation s 
                : m_stateManager.getElementAddressBlocks( serialNumber ) )
        {
            final ElementAddressBlockInformation info =
                    BeanFactory.newBean( ElementAddressBlockInformation.class );
            BeanCopier.copy( info, s );
            retval.add( info );
        }
        
        return CollectionFactory.toArray( ElementAddressBlockInformation.class, retval );
    }

    
    private void initializeTapePartitionResources()
    {
        synchronized ( m_stateManager )
        {
            final Set< String > newInstanceSerialNumbers = new HashSet<>();
            for ( final SimPartition s : m_stateManager.getPartitions( null ) )
            {
                newInstanceSerialNumbers.add( s.getSerialNumber() );
            }
            
            for ( final String serialNumber 
                    : new HashSet<>( m_lastInitializedTapePartitionResourceInstances.keySet() ) )
            {
                if ( newInstanceSerialNumbers.contains( serialNumber ) )
                {
                    newInstanceSerialNumbers.remove( serialNumber );
                }
                else
                {
                    m_rpcServer.unregister( 
                            m_lastInitializedTapePartitionResourceInstances.get( serialNumber ), 
                            TapePartitionResource.class );
                    m_lastInitializedTapePartitionResourceInstances.remove( serialNumber );
                }
            }
            
            for ( final String serialNumber : newInstanceSerialNumbers )
            {
                if ( null == m_stateManager.getPartition( serialNumber ) )
                {
                    throw new IllegalStateException( "Tape partition doesn't exist: " + serialNumber );
                }
                m_rpcServer.register(
                        serialNumber,
                        SerialAccessRpcResourceFactory.asSerialResource(
                                TapePartitionResource.class,
                                new SimTapePartitionResource( serialNumber ) ) );
                m_lastInitializedTapePartitionResourceInstances.put( serialNumber, serialNumber );
            }
        }
    }   
    
    
    private final class SimTapePartitionResource extends BaseRpcResource implements TapePartitionResource
    {
        private SimTapePartitionResource( final String serialNumber )
        {
            m_serialNumber = serialNumber;
        }
        
        public RpcFuture< ? > move( final int srcElementAddress, final int destElementAddress )
        {
            return SimTapeEnvironmentResource.this.move(
                    m_serialNumber, 
                    srcElementAddress, 
                    destElementAddress );
        }
        
        private final String m_serialNumber;
    } // end inner class def

    
    private void initializeTapeDriveResources()
    {
        synchronized ( m_stateManager )
        {
            final Set< String > newInstanceSerialNumbers = new HashSet<>();
            for ( final SimDrive s : m_stateManager.getDrives( null ) )
            {
                newInstanceSerialNumbers.add( s.getSerialNumber() );
            }
            
            for ( final String serialNumber 
                    : new HashSet<>( m_lastInitializedTapeDriveResourceInstances.keySet() ) )
            {
                if ( newInstanceSerialNumbers.contains( serialNumber ) )
                {
                    newInstanceSerialNumbers.remove( serialNumber );
                }
                else
                {
                    m_rpcServer.unregister( 
                            m_lastInitializedTapeDriveResourceInstances.get( serialNumber ), 
                            SimTapeDriveResource.class );
                    m_stateManager.unregisterTapeDriveResource( serialNumber );
                    m_lastInitializedTapeDriveResourceInstances.remove( serialNumber );
                }
            }
            
            for ( final String serialNumber : newInstanceSerialNumbers )
            {
                if ( null == m_stateManager.getDrive( serialNumber ) )
                {
                    throw new IllegalStateException( "Tape drive doesn't exist: " + serialNumber );
                }
                SimTapeDriveResource driveResource = new SimTapeDriveResource(
                        serialNumber,
                        m_stateManager);
                m_rpcServer.register(
                        serialNumber,
                        SerialAccessRpcResourceFactory.asSerialResource(
                                TapeDriveResource.class,
                                driveResource) );
                m_stateManager.registerTapeDriveResource( serialNumber, driveResource );
                m_lastInitializedTapeDriveResourceInstances.put( serialNumber, serialNumber );
            }
        }
    }

    synchronized public TapeDriveResource swapTapeDriveResource(final String serialNumber, final TapeDriveResource newResource) {
        final TapeDriveResource oldResource = m_stateManager.getTapeDriveResource(serialNumber);
        m_stateManager.registerTapeDriveResource(serialNumber, newResource);
        m_rpcServer.register(serialNumber, newResource);
        return oldResource;
    }
    
    public RpcFuture< ? > move(
            final String partitionSerialNumber,
            final int srcElementAddress, 
            final int destElementAddress )
    {
        move( partitionSerialNumber, srcElementAddress, destElementAddress, 5 );
        return null;
    }
    
    
    private void move(
            final String partitionSerialNumber,
            final int srcElementAddress, 
            final int destElementAddress,
            final int numRetriesRemaining )
    {
        m_stateManager.simulateDelay( m_stateManager.getSimulatorConfig().getMoveDelay(), "move " + m_stateManager.getTape(partitionSerialNumber, srcElementAddress) + " from " + srcElementAddress + " to " + destElementAddress);
        synchronized ( m_stateManager )
        {
            moveInternal( partitionSerialNumber, srcElementAddress, destElementAddress, numRetriesRemaining );
        }
    }
    
    
    private void moveInternal(
            final String partitionSerialNumber,
            final int srcElementAddress, 
            final int destElementAddress,
            final int numRetriesRemaining )
    {
        verifyEnvironmentHasNotChanged();
        
        final ElementAddressType srcType = getAddressType( partitionSerialNumber, srcElementAddress );
        final SimTape srcTape = getTape( partitionSerialNumber, srcElementAddress );
        final SimDrive srcDrive = getDrive( partitionSerialNumber, srcElementAddress );
        validateElementAddress( srcType, srcDrive );
        if ( null == srcTape && null == srcDrive )
        {
            throw new IllegalStateException( 
                    "No tape or drive found at source element address." );
        }

        final ElementAddressType destType = getAddressType( partitionSerialNumber, destElementAddress );
        final SimTape destTape = getTape( partitionSerialNumber, destElementAddress );
        final SimDrive destDrive = getDrive( partitionSerialNumber, destElementAddress );
        validateElementAddress( destType, destDrive );
        if ( null != destTape && null != destDrive )
        {
            throw new IllegalStateException( 
                    "Cannot have a tape and drive at the same destination element address." );
        }
        
        if ( ElementAddressType.TAPE_DRIVE == srcType )
        {
            if ( ElementAddressType.STORAGE == destType || ElementAddressType.IMPORT_EXPORT == destType )
            {
                if ( null == srcTape )
                {
                    throw new IllegalStateException( "No tape to remove from drive." );
                }
                if ( !srcTape.isRemovalPrepared() )
                {
                    if ( 0 >= numRetriesRemaining )
                    {
                        throw new IllegalStateException(
                                "You need to prepare the tape for removal before you remove the tape." );
                    }
                    try
                    {
                        Thread.sleep( 10 );
                    }
                    catch ( final InterruptedException ex )
                    {
                        throw new RuntimeException( ex );
                    }
                    move( partitionSerialNumber, 
                          srcElementAddress, 
                          destElementAddress, 
                          numRetriesRemaining - 1 );
                    return;
                }
                removeTapeFromDrive(
                        srcTape, 
                        destElementAddress );
                return;
            }
        }
        
        if ( ElementAddressType.TAPE_DRIVE == destType )
        {
            if ( ElementAddressType.STORAGE == srcType || ElementAddressType.IMPORT_EXPORT == srcType )
            {
                loadTapeIntoDrive( srcTape, destElementAddress );
                return;
            }
        }
        
        if ( ElementAddressType.STORAGE == srcType || ElementAddressType.IMPORT_EXPORT == srcType )
        {
            if ( ElementAddressType.STORAGE == destType || ElementAddressType.IMPORT_EXPORT == destType )
            {
                moveTapeInStorage( srcTape, destElementAddress );
                return;
            }
        }
        
        throw new UnsupportedOperationException( 
                "No code written to move from " + srcType + " to " + destType + "." );
    }
    
    
    private void validateElementAddress(
            final ElementAddressType type,
            final SimDrive drive )
    {
        switch ( type )
        {
            case ROBOT:
                throw new UnsupportedOperationException( "No code written to support: " + type );
            case IMPORT_EXPORT:
                if ( null != drive )
                {
                    throw new IllegalStateException( "Type was " + type + ", but drive was at location." );
                }
                break;
            case STORAGE:
                if ( null != drive )
                {
                    throw new IllegalStateException( "Type was " + type + ", but drive was at location." );
                }
                break;
            case TAPE_DRIVE:
                if ( null == drive )
                {
                    throw new IllegalStateException( "Type was " + type + ", but drive was null." );
                }
                break;
            default:
                throw new UnsupportedOperationException( "No code written to support: " + type );
        }
    }
    
    
    private SimTape getTape( final String partitionSerialNumber, final int elementAddress )
    {
        for ( final SimTape tape : m_stateManager.getTapes( partitionSerialNumber ) )
        {
            if ( tape.getElementAddress() == elementAddress )
            {
                return tape;
            }
        }
        return null;
    }
    
    
    private SimDrive getDrive( final String partitionSerialNumber, final int elementAddress )
    {
        for ( final SimDrive drive : m_stateManager.getDrives( partitionSerialNumber ) )
        {
            if ( drive.getElementAddress() == elementAddress )
            {
                return drive;
            }
        }
        return null;
    }
    
    
    private ElementAddressType getAddressType( final String partitionSerialNumber, final int elementAddress )
    {
        final List< ElementAddressType > retval = new ArrayList<>();
        for ( final ElementAddressBlockInformation block 
                : m_stateManager.getElementAddressBlocks( partitionSerialNumber ) )
        {
            if ( elementAddress >= block.getStartAddress() && elementAddress <= block.getEndAddress() )
            {
                retval.add( block.getType() );
            }
        }
        
        if ( retval.isEmpty() )
        {
            throw new IllegalStateException( "Element address is not in range of any valid address block." );
        }
        if ( 1 != retval.size() )
        {
            throw new IllegalStateException( 
                    "Element address falls inside multiple valid address blocks: " + retval );
        }
        return retval.get( 0 );
    }

    
    private void loadTapeIntoDrive( final SimTape tape, final int destElementAddress )
    {
        Validations.verifyNotNull( "Tape", tape );
        final SimDrive drive =
                m_stateManager.getDrive( tape.getPartitionSerialNumber(), destElementAddress );
        if ( null != drive.getTapeSerialNumber() )
        {
            throw new IllegalStateException( "A tape is currently loaded in tape drive." );
        }
        drive.setTapeSerialNumber( tape.getHardwareSerialNumber() );
        tape.setElementAddress( drive.getElementAddress() );
        tape.setRemovalPrepared( false );
    }

    
    private void removeTapeFromDrive( final SimTape tape, final int destElementAddress )
    {
        Validations.verifyNotNull( "Tape", tape );
        final SimDrive drive =
                m_stateManager.getDrive( tape.getPartitionSerialNumber(), tape.getElementAddress() );
        if ( tape.isQuiesceRequired() )
        {
            throw new IllegalStateException(
                    "You need to quiesce the tape before you remove it from the drive." );
        }
        if ( !tape.getHardwareSerialNumber().equals( drive.getTapeSerialNumber() ) )
        {
            throw new IllegalStateException( 
                    "Tape is not loaded in tape drive.  Tape drive has loaded: " 
                    + drive.getTapeSerialNumber() );
        }
        if ( null != m_stateManager.getTape( tape.getPartitionSerialNumber(), destElementAddress ) )
        {
            throw new IllegalStateException(
                    "A tape is already at element address " + destElementAddress + "." );
        }
        drive.setTapeSerialNumber( null );
        tape.setElementAddress( destElementAddress );
    }

    
    private void moveTapeInStorage( final SimTape tape, final int destElementAddress )
    {
        Validations.verifyNotNull( "Tape", tape );
        for ( final SimDrive drive : m_stateManager.getDrives( tape.getPartitionSerialNumber() ) )
        {
            if ( tape.getHardwareSerialNumber().equals( drive.getTapeSerialNumber() ) )
            {
                throw new IllegalStateException( 
                        "You cannot call this method when the tape is in a drive." );
            }
        }
        if ( null != m_stateManager.getTape( tape.getPartitionSerialNumber(), destElementAddress ) )
        {
            throw new IllegalStateException(
                    "A tape is already at element address " + destElementAddress + "." );
        }
        tape.setElementAddress( destElementAddress );
    }
    
    
    private void verifyEnvironmentHasNotChanged()
    {
        if ( m_stateManager.hasChanged() )
        {
            throw new SimulatorException(
                    SimulatorFailure.TAPE_ENVIRONMENT_CHANGED, 
                    "The tape environment has changed." );
        }
    }

    private final AtomicLong m_tapeEnvironmentGenerationNumber = new AtomicLong();
    private final SimStateManager m_stateManager;
    private final RpcServer m_rpcServer;
    private final Map< String, String > m_lastInitializedTapeDriveResourceInstances = new HashMap<>();
    private final Map< String, String > m_lastInitializedTapePartitionResourceInstances = new HashMap<>();
    private final static Logger LOG = Logger.getLogger( SimTapeEnvironmentResource.class );
}
