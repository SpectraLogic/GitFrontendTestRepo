/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator.state.simresource;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;

import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.TapeEnvironmentResource;
import com.spectralogic.s3.simulator.Simulator;
import com.spectralogic.s3.simulator.lang.PoolConfig;
import com.spectralogic.s3.simulator.lang.SimulatorConfig;
import com.spectralogic.s3.simulator.taperesource.ErrorSimTapeDriveResource;
import com.spectralogic.s3.simulator.taperesource.ErrorSimTapeEnvironmentResource;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.rpc.tape.domain.ElementAddressBlockInformation;
import com.spectralogic.s3.simulator.domain.SimDrive;
import com.spectralogic.s3.simulator.domain.SimLibrary;
import com.spectralogic.s3.simulator.domain.SimPartition;
import com.spectralogic.s3.simulator.domain.SimTape;
import com.spectralogic.s3.simulator.state.SimStateManager;
import com.spectralogic.s3.simulator.taperesource.SimTapeEnvironmentResource;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseRpcResource;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.net.rpc.server.RpcServer;

import static com.spectralogic.s3.simulator.Simulator.*;

public final class SimStateManagerImpl extends BaseRpcResource implements SimStateManager
{
    public SimStateManagerImpl(final RpcServer rpcServer, final SimulatorConfig config)
    {
        Validations.verifyNotNull( "RPC Server", rpcServer );

        m_rpcServer = rpcServer;
        m_config = config;
        m_simTapeEnvironmentResource = new SimTapeEnvironmentResource( this, m_rpcServer );
        m_rpcServer.register(
                null,
                 m_simTapeEnvironmentResource);
        populateSimHardware( config );
    }

    public SimStateManagerImpl( final RpcServer rpcServer )
    {
        this( rpcServer, Simulator.getTestConfig() );
    }


    synchronized public TapeEnvironmentResource swapTapeEnvironmentResource(final TapeEnvironmentResource newResource) {
        final TapeEnvironmentResource oldResource = m_simTapeEnvironmentResource;
        m_simTapeEnvironmentResource = newResource;
        m_rpcServer.register(
                null,
                m_simTapeEnvironmentResource);
        return oldResource;
    }

    @Override
    public TapeDriveResource swapTapeDriveResource(String serialNumber, TapeDriveResource resource) {
        if (m_simTapeEnvironmentResource instanceof SimTapeEnvironmentResource) {
            final SimTapeEnvironmentResource tapeEnvironmentResource =
                    (SimTapeEnvironmentResource) m_simTapeEnvironmentResource;
            return tapeEnvironmentResource.swapTapeDriveResource(serialNumber, resource);
        } else {
            throw new IllegalStateException("swapTapeDriveResource not available for " +
                    m_simTapeEnvironmentResource.getClass().getSimpleName() );
        }
    }

    synchronized public void swapErrorTapeEnvironmentResource() {
        m_simTapeEnvironmentResource = new ErrorSimTapeEnvironmentResource();
        m_rpcServer.register(
                null,
                new ErrorSimTapeEnvironmentResource());

    }

    synchronized public void swapGoodTapeEnvironmentResource() {
        final SimStateManager stateManager = new SimStateManagerImpl( m_rpcServer, m_config );
        m_rpcServer.register( null, stateManager );

        final SimTapeEnvironmentResource mediaChangerResource =
                new SimTapeEnvironmentResource( stateManager, m_rpcServer );
        m_simTapeEnvironmentResource = mediaChangerResource;
        m_rpcServer.register(
                null,
                mediaChangerResource );

    }



    synchronized public SimulatorConfig  swapConfig(final SimulatorConfig config) {
        SimulatorConfig existing = m_config;
        m_config = config;
        return existing;
    }


    synchronized public SimDevices swapSimDevices(final SimDevices newDevices) {
        final SimDevices oldDevices = m_simDevices;
        m_simDevices = newDevices;
        return oldDevices;
    }

    synchronized public RpcFuture< SimLibrary > addLibrary( final SimLibrary library )
    {
        m_changed = true;
        Validations.verifyNotNull( "Library", library );
        Validations.verifyNotNull( "Serial number", library.getSerialNumber() );
        m_simDevices.getLibraries().put( library.getSerialNumber(), library );
        return new RpcResponse<>( library );
    }
        
        
    synchronized public RpcFuture< SimPartition > addPartition( final SimPartition partition )
    {
        m_changed = true;
        Validations.verifyNotNull( "Partition", partition );
        Validations.verifyNotNull( "Serial number", partition.getSerialNumber() );
        Validations.verifyNotNull( "Library serial number", partition.getLibrarySerialNumber() );
        if ( m_simDevices.getPartitions().containsKey( partition.getSerialNumber() ) )
        {
            throw new IllegalStateException( 
                    "Partition " + partition.getSerialNumber() + " already exists." );
        }
        
        final File dir = new File(m_config.getVirtualLibraryPath() );
        if ( !dir.exists() )
        {
            dir.mkdirs();
        }
        if ( !dir.isDirectory() )
        {
            throw new IllegalArgumentException( "Path must be a directory: " + dir );
        }
        
        m_simDevices.getPartitions().put( partition.getSerialNumber(), partition );
        LOG.info( "Partition '" + partition.getSerialNumber() + "' added.  Simulated tapes will be under: " 
                  + dir );
        
        return new RpcResponse<>( partition );
    }

    
    synchronized public RpcFuture< SimPartition > updatePartition( 
            final String serialNumber, 
            final String message,
            final boolean online )
    {
        Validations.verifyNotNull( "Serial number", serialNumber );
        
        m_changed = true;
        if ( !m_simDevices.getPartitions().containsKey( serialNumber ) )
        {
            throw new IllegalStateException( "Partition doesn't exist: " + serialNumber );
        }
        
        m_simDevices.getPartitions().get( serialNumber ).setOnline( online );
        m_simDevices.getPartitions().get( serialNumber ).setErrorMessage( message );
        return new RpcResponse<>( m_simDevices.getPartitions().get( serialNumber ) );
    }

    
    synchronized public RpcFuture< SimDrive > addDrive( final SimDrive tapeDrive )
    {
        m_changed = true;
        Validations.verifyNotNull( "Tape drive", tapeDrive );
        Validations.verifyNotNull( "Partition serial number", tapeDrive.getPartitionSerialNumber() );
        Validations.verifyNotNull( "Serial number", tapeDrive.getSerialNumber() );
        Validations.verifyNotNull( "Type", tapeDrive.getType() );
        if ( m_simDevices.getDrives().containsKey( tapeDrive.getSerialNumber() ) )
        {
            throw new IllegalStateException( 
                    "Drive " + tapeDrive.getSerialNumber() + " already exists." );
        }
        for ( final SimDrive d : m_simDevices.getDrives().values() )
        {
            if ( tapeDrive.getPartitionSerialNumber().equals( d.getPartitionSerialNumber() )
                    && tapeDrive.getElementAddress() == d.getElementAddress() )
            {
                throw new IllegalStateException( 
                        "There's already a drive at address " + tapeDrive.getElementAddress() + "." );
            }
        }
        
        m_simDevices.getDrives().put( tapeDrive.getSerialNumber(), tapeDrive );
        return new RpcResponse<>( tapeDrive );
    }

    
    synchronized public RpcFuture< SimDrive > updateDrive( 
            final String serialNumber, 
            final String message, 
            final boolean online )
    {
        Validations.verifyNotNull( "Serial number", serialNumber );
        m_changed = true;
        if ( !m_simDevices.getDrives().containsKey( serialNumber ) )
        {
            throw new IllegalStateException( "Drive doesn't exist: " + serialNumber );
        }
        
        m_simDevices.getDrives().get( serialNumber ).setOnline( online );
        m_simDevices.getDrives().get( serialNumber ).setErrorMessage( message );
        return new RpcResponse<>( m_simDevices.getDrives().get( serialNumber ) );
    }

    
    synchronized public RpcFuture< SimTape > addTape( final SimTape tape )
    {
        m_changed = true;
        Validations.verifyNotNull( "Tape", tape );
        Validations.verifyNotNull( "Partition serial number", tape.getPartitionSerialNumber() );
        Validations.verifyNotNull( "Bar code", tape.getBarCode() );
        Validations.verifyNotNull( "Type", tape.getType() );
        Validations.verifyInRange(
                "Available capacity", 0, tape.getTotalRawCapacity(), tape.getAvailableRawCapacity() );
        Validations.verifyInRange(
                "Total capacity", 1, Long.MAX_VALUE, tape.getTotalRawCapacity() );
        if ( null == tape.getHardwareSerialNumber() )
        {
            tape.setHardwareSerialNumber( tape.getBarCode() );
        }
        if ( m_simDevices.getTapes().containsKey( tape.getHardwareSerialNumber() ) )
        {
            throw new IllegalStateException( 
                    "Tape " + tape.getBarCode() + " already exists." );
        }
        for ( final SimTape t : m_simDevices.getTapes().values() )
        {
            if ( tape.getPartitionSerialNumber().equals( t.getPartitionSerialNumber() )
                    && tape.getElementAddress() == t.getElementAddress() )
            {
                throw new IllegalStateException( 
                        "There's already a tape at address " + tape.getElementAddress() + "." );
            }
        }
        
        m_simDevices.getTapes().put( tape.getHardwareSerialNumber(), tape );
        return new RpcResponse<>( tape );
    }

    
    synchronized public RpcFuture< SimTape > updateTape( final String serialNumber, final boolean online )
    {
        m_changed = true;
        if ( !m_simDevices.getTapes().containsKey( serialNumber ) )
        {
            throw new IllegalStateException( "Tape doesn't exist: " + serialNumber );
        }
        
        m_simDevices.getTapes().get( serialNumber ).setOnline( online );
        return new RpcResponse<>( m_simDevices.getTapes().get( serialNumber ) );
    }
    

    synchronized public RpcFuture< ? > setElementAddressBlocks( 
            final String partitionSerialNumber,
            final ElementAddressType type,
            final ElementAddressBlockInformation [] blocksForType )
    {
        Validations.verifyNotNull( "Partition serial number", partitionSerialNumber );
        Validations.verifyNotNull( "Type", type );
        if ( !m_simDevices.getElementAddressBlocks().containsKey( partitionSerialNumber ) )
        {
            m_simDevices.getElementAddressBlocks().put( 
                    partitionSerialNumber, 
                    new HashMap< ElementAddressType, Set< ElementAddressBlockInformation > >() );
        }
        m_simDevices.getElementAddressBlocks().get( partitionSerialNumber ).put(
                type, 
                CollectionFactory.toSet( blocksForType ) );
        validateElementAddressBlocks();
        return null;
    }
    

    synchronized public RpcFuture< ? > addElementAddressBlock( 
            final String partitionSerialNumber,
            final ElementAddressType type,
            final int startAddress,
            final int endAddress )
    {
        Validations.verifyNotNull( "Partition serial number", partitionSerialNumber );
        Validations.verifyNotNull( "Type", type );
        if ( !m_simDevices.getElementAddressBlocks().containsKey( partitionSerialNumber ) )
        {
            m_simDevices.getElementAddressBlocks().put( 
                    partitionSerialNumber, 
                    new HashMap< ElementAddressType, Set< ElementAddressBlockInformation > >() );
        }
        
        final ElementAddressBlockInformation block = 
                BeanFactory.newBean( ElementAddressBlockInformation.class );
        block.setType( type );
        block.setStartAddress( startAddress );
        block.setEndAddress( endAddress );
        
        if ( !m_simDevices.getElementAddressBlocks().get( partitionSerialNumber ).containsKey( type ) )
        {
            m_simDevices.getElementAddressBlocks().get( partitionSerialNumber ).put(
                    type, 
                    new HashSet< ElementAddressBlockInformation >() );
        }
        final Set< ElementAddressBlockInformation > blocks = 
                m_simDevices.getElementAddressBlocks().get( partitionSerialNumber ).get( type );
        blocks.add( block );
        m_simDevices.getElementAddressBlocks().get( partitionSerialNumber ).put( type, blocks );
        validateElementAddressBlocks();
        
        return null;
    }
    
    
    private void validateElementAddressBlocks()
    {
        for ( final Map.Entry< String, Map< ElementAddressType, Set< ElementAddressBlockInformation > > > e 
                : m_simDevices.getElementAddressBlocks().entrySet() )
        {
            validateElementAddressBlocks( e.getValue() );
        }
    }
    
    
    private void validateElementAddressBlocks(
            final Map< ElementAddressType, Set< ElementAddressBlockInformation > > allBlocks )
    {
        final Map< Integer, ElementAddressType > addresses = new HashMap<>();
        for ( final Map.Entry< ElementAddressType, Set< ElementAddressBlockInformation > > e 
                : allBlocks.entrySet() )
        {
            for ( final ElementAddressBlockInformation block : e.getValue() )
            {
                if ( e.getKey() != block.getType() )
                {
                    throw new IllegalStateException( 
                            "Element address block configuration is invalid.  Block has type " 
                            + block.getType() + ", but expected it to be " + e.getKey() + "." );
                }
                if ( block.getStartAddress() > block.getEndAddress() )
                {
                    throw new IllegalStateException(
                            "Element address block configuration is invalid.  Block start address (" 
                            + block.getStartAddress() + ") is greater than end address (" 
                            + block.getEndAddress() + ")." );
                }
                for ( int i = block.getStartAddress(); i <= block.getEndAddress(); ++i )
                {
                    if ( addresses.containsKey( Integer.valueOf( i ) ) )
                    {
                        throw new IllegalStateException( 
                                "Element address " + i + " is already allocated as a " 
                                + addresses.get( Integer.valueOf( i ) ) + "." );
                    }
                    addresses.put( Integer.valueOf( i ), e.getKey() );
                }
            }
        }
    }
    
    
    public void simulateDelay( final int maxMillis )
    {
        final int delayInMillis = ( 0 >= maxMillis ) ?
                1 
                : new SecureRandom().nextInt( maxMillis );
        try
        {
            Thread.sleep( delayInMillis );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
    }


    public void simulateDelay( final int delayInMillis, final String action )
    {
        LOG.info("Delaying for " + delayInMillis + " to simulate " + action);
        try
        {
            Thread.sleep( delayInMillis );
        }
        catch ( final InterruptedException ex )
        {
            throw new RuntimeException( ex );
        }
        LOG.info("Simulating " + action + " complete.");
    }

    @Override
    public SimulatorConfig getSimulatorConfig() {
        return m_config;
    }


    synchronized public boolean hasChanged()
    {
        if ( m_changed )
        {
            m_changed = false;
            return true;
        } else if ( !m_simulateRandomEnvironmentChanges )
        {
            return false;
        }
        
        if ( 0 < m_minimumFalseRepliesBeforeNextPossibleTrue)
        {
            // Some tests, most notably two in TapeBlobStoreIntegration_Test (at
            // least at the time of this writing), fail "mysteriously", due to
            // correct application code, if this random algo returns more than
            // a couple 'true' in a row. Thus, we make sure this never happens.
            --m_minimumFalseRepliesBeforeNextPossibleTrue;
            return false;
        }
        final boolean rv = ( 0 == new SecureRandom().nextInt( 5 ) );
        if ( rv )
        {
            // The nextInt( 5 ) and the 7 here keeps the long
            // term rate of random true returns at ~8%.
            m_minimumFalseRepliesBeforeNextPossibleTrue = 7;
        }
        return rv;
    }

    
    public SimLibrary getLibrary( final String serialNumber )
    {
        for ( final SimLibrary s : getLibraries() )
        {
            if ( serialNumber.equals( s.getSerialNumber() ) )
            {
                return s;
            }
        }
        return null;
    }

    
    public SimPartition getPartition( final String serialNumber )
    {
        for ( final SimPartition s : getPartitions( null ) )
        {
            if ( serialNumber.equals( s.getSerialNumber() ) )
            {
                return s;
            }
        }
        return null;
    }


    public SimDrive getDrive( final String serialNumber )
    {
        for ( final SimDrive s : getDrives( null ) )
        {
            if ( serialNumber.equals( s.getSerialNumber() ) )
            {
                return s;
            }
        }
        return null;
    }
    
    
    public SimDrive getDrive( final String partitionSerialNumber, final int elementAddress )
    {
        for ( final SimDrive s : getDrives( partitionSerialNumber ) )
        {
            if ( elementAddress == s.getElementAddress() )
            {
                return s;
            }
        }
        return null;
    }


    public SimTape getTape( final String serialNumber )
    {
        for ( final SimTape s : getTapes( null ) )
        {
            if ( serialNumber.equals( s.getHardwareSerialNumber() ) )
            {
                return s;
            }
        }
        return null;
    }


    public SimTape getTape( final String partitionSerialNumber, final int elementAddress )
    {
        Validations.verifyNotNull( "Partition serial number", partitionSerialNumber );
        for ( final SimTape s : getTapes( partitionSerialNumber ) )
        {
            if ( elementAddress == s.getElementAddress() 
                    && partitionSerialNumber.equals( s.getPartitionSerialNumber() ) )
            {
                return s;
            }
        }
        return null;
    }
    
    
    synchronized public Set< SimLibrary > getLibraries()
    {
        final Set< SimLibrary > retval = new HashSet<>();
        
        for ( final SimLibrary s : m_simDevices.getLibraries().values() )
        {
            retval.add( s );
        }
        
        return retval;
    }
    
    
    synchronized public Set< SimPartition > getPartitions( final String librarySerialNumber )
    {
        final Set< SimPartition > retval = new HashSet<>();
        
        for ( final SimPartition s : m_simDevices.getPartitions().values() )
        {
            if ( s.isOnline() && ( null == librarySerialNumber 
                    || librarySerialNumber.equals( s.getLibrarySerialNumber() ) ) )
            {
                retval.add( s );
            }
        }
        
        return retval;
    }


    private void populateSimHardware(final SimulatorConfig config)
    {
        LOG.info( Platform.NEWLINE + Platform.NEWLINE +
                "Initializing test Simulator state." );
        final SimLibrary library = BeanFactory.newBean( SimLibrary.class );
        library.setSerialNumber( DEFAULT_LIBRARY_SN );
        addLibrary( library );

        int dIdx = 0;
        int tIdx = 0;
        for (int p = 0; p < config.getNumPartitions(); p++) {
            final String partitionSn = DEFAULT_PARTITION_SN + p;
            final SimPartition partition = BeanFactory.newBean( SimPartition.class );
            partition.setSerialNumber( partitionSn );
            partition.setLibrarySerialNumber( DEFAULT_LIBRARY_SN );
            partition.setOnline( true );
            partition.setDriveType(config.getDriveType());
            addPartition( partition );


            final int numDrives = config.getDrivesPerPartition();
            final int numTapes = config.getTapesPerPartition();

            m_startDriveRange = 0;
            m_endDriveRange = numDrives + 100;
            m_startTapeRange = m_endDriveRange + 100;
            m_endTapeRange = m_startTapeRange + numTapes + 1000;
            m_startIErange = m_endTapeRange + 100;
            m_endIErange = m_startIErange + 1000;

            for (int d = 0; d < numDrives; d++) {
                final SimDrive drive = BeanFactory.newBean( SimDrive.class );
                drive.setPartitionSerialNumber( partition.getSerialNumber() );
                drive.setSerialNumber( DEFAULT_DRIVE_SN + dIdx );
                drive.setMfgSerialNumber( DEFAULT_DRIVE_SN + dIdx );
                drive.setType( config.getDriveType() );
                drive.setElementAddress( d );
                addDrive( drive );
                dIdx++;
            }

            for (int t = 0; t < numTapes; t++) {
                final SimTape tape = BeanFactory.newBean( SimTape.class );
                tape.setBarCode( DEFAULT_TAPE_SN + tIdx );
                tape.setPartitionSerialNumber( partition.getSerialNumber() );
                tape.setTotalRawCapacity(config.getTapeType().getMaxCapacity());
                tape.setHardwareSerialNumber( DEFAULT_TAPE_SN + ( tIdx ) );
                tape.setSerialNumber(tape.getHardwareSerialNumber());
                tape.setOnline(true);
                tape.setAvailableRawCapacity( tape.getTotalRawCapacity() );
                tape.setElementAddress( m_startTapeRange + t );
                tape.setType( config.getTapeType() );
                addTape( tape );
                tIdx++;
            }

            addElementAddressBlock(
                    partitionSn, ElementAddressType.TAPE_DRIVE, m_startDriveRange, m_endDriveRange);
            addElementAddressBlock(
                    partitionSn, ElementAddressType.STORAGE, m_startTapeRange, m_endTapeRange );
            addElementAddressBlock(
                    partitionSn, ElementAddressType.IMPORT_EXPORT, m_startIErange, m_endIErange );
        }

    }


    synchronized public Set< SimDrive > getDrives( final String partitionSerialNumber )
    {
        final Set< SimDrive > retval = new HashSet<>();
        
        for ( final SimDrive s : m_simDevices.getDrives().values() )
        {
            final SimPartition p = m_simDevices.getPartitions().get( s.getPartitionSerialNumber() );
            if ( null != p && p.isOnline() )
            {
                if ( s.isOnline() && ( null == partitionSerialNumber 
                        || partitionSerialNumber.equals( s.getPartitionSerialNumber() ) ) )
                {
                    retval.add( s );
                }
            }
        }
        
        return retval;
    }
    
    
    synchronized public Set< SimTape > getTapes( final String partitionSerialNumber )
    {
        final Set< SimTape > retval = new HashSet<>();
        
        for ( final SimTape s : m_simDevices.getTapes().values() )
        {
            final SimPartition p = m_simDevices.getPartitions().get( s.getPartitionSerialNumber() );
            if ( null != p && p.isOnline() )
            {
                if ( s.isOnline() && ( null == partitionSerialNumber 
                        || partitionSerialNumber.equals( s.getPartitionSerialNumber() ) ) )
                {
                    retval.add( s );
                }
            }
        }
        
        return retval;
    }   
    
    
    synchronized public Set< ElementAddressBlockInformation > getElementAddressBlocks( 
            final String partitionSerialNumber )
    {
        Validations.verifyNotNull( "Partition serial number", partitionSerialNumber );
        validateElementAddressBlocks();
        final Set< ElementAddressBlockInformation > retval = new HashSet<>();
        if ( !m_simDevices.getElementAddressBlocks().containsKey( partitionSerialNumber ) )
        {
            return null;
        }
        
        for ( final Set< ElementAddressBlockInformation > blocks
                : m_simDevices.getElementAddressBlocks().get( partitionSerialNumber ).values() )
        {
            for ( final ElementAddressBlockInformation block : blocks )
            {
                retval.add( block );
            }
        }
        
        return retval;
    }
    

    synchronized public RpcFuture< ? > reset(
            final int numberOfPartitions,
            final int drivesPerPartition,
            final int tapesPerPartition,
            String partitionRootPath )
    {
        m_simDevices.getLibraries().clear();
        m_simDevices.getPartitions().clear();
        m_simDevices.getDrives().clear();
        m_simDevices.getTapes().clear();
        m_simDevices.getElementAddressBlocks().clear();

        final SimulatorConfig config = Simulator.getTestConfig()
                .setNumPartitions( numberOfPartitions )
                .setDrivesPerPartition( drivesPerPartition )
                .setTapesPerPartition( tapesPerPartition );
        populateSimHardware( config );
        return null;
    }

    public static void deleteNonUnderscoreFiles(File directory) {
        // Use Objects.requireNonNull to avoid potential NullPointerException
        for (File file : Objects.requireNonNull(directory.listFiles())) {
            if (file.isDirectory()) {
                // Recursive call for subdirectories
                deleteNonUnderscoreFiles(file);
            } else {
                // Check if the file name does NOT start with "_"
                if (!file.getName().startsWith("_")) {
                    LOG.info("Deleting file: {}" + file.getName()); // Use SLF4J
                    if (file.delete()) {
                        LOG.info("Successfully deleted: {}"+ file.getName());
                    } else {
                        LOG.error("Failed to delete: {}"+ file.getName());
                    }
                } else {
                    LOG.info("Skipping file: {}"+ file.getName());
                }
            }
        }
    }

    public void cleanup() {
        LOG.info("Delete tapes on startup enabled in config. Will attempt to delete all files in virtual library directory: " + m_config.getVirtualLibraryPath());
        try {
            final File f = new File(m_config.getVirtualLibraryPath());
            if (f.exists()) {
                LOG.info("Deleting virtual library directory: ----------" + m_config.getVirtualLibraryPath() + f.exists());
                for (File file : Objects.requireNonNull(f.listFiles())) {
                    deleteNonUnderscoreFiles(file);
                }
            }

        } catch (Exception e) {
            LOG.warn("Could not delete files in library directory: " + m_config.getVirtualLibraryPath(), e);
        }
        LOG.info("Deleted all files in virtual library directory.");
    }

    public int getStartDriveRange() {
        return m_startDriveRange;
    }

    public int getEndDriveRange() {
        return m_endDriveRange;
    }

    public int getStartTapeRange() {
        return m_startTapeRange;
    }

    public int getEndTapeRange() {
        return m_endTapeRange;
    }

    public int getStartIERange() {
        return m_startIErange;
    }

    public int getEndIERange() {
        return m_endIErange;
    }

    synchronized public void registerTapeDriveResource(String serialNumber, TapeDriveResource resource) {
        m_simTapeDriveResources.put(serialNumber, resource);
    }

    synchronized public TapeDriveResource unregisterTapeDriveResource(String serialNumber) {
        return m_simTapeDriveResources.remove(serialNumber);
    }

    synchronized public TapeDriveResource getTapeDriveResource(String serialNumber) {
        return m_simTapeDriveResources.get(serialNumber);
    }

    synchronized public Map<String, TapeDriveResource> getTapeDriveResources() {
        return new HashMap<>(m_simTapeDriveResources);
    }

    private TapeEnvironmentResource m_simTapeEnvironmentResource;
    private SimDevices m_simDevices = new SimDevices();
    private final Map<String, TapeDriveResource> m_simTapeDriveResources = new HashMap<>();
    private boolean m_changed;
    private int m_minimumFalseRepliesBeforeNextPossibleTrue = 0;
    private int m_startDriveRange = 0;
    private int m_endDriveRange = 0;
    private int m_startTapeRange = 0;
    private int m_endTapeRange = 0;
    private int m_startIErange = 0;
    private int m_endIErange = 0;
    private final boolean m_simulateRandomEnvironmentChanges = false;
    private final RpcServer m_rpcServer;
    private  SimulatorConfig m_config;
    
    private final static Logger LOG = Logger.getLogger( SimStateManagerImpl.class );
}
