/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator;

import java.io.File;
import java.io.FileOutputStream;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.LtfsFileNamingMode;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.rpc.RpcServerPort;
import com.spectralogic.s3.common.rpc.tape.TapeDriveResource;
import com.spectralogic.s3.common.rpc.tape.TapeEnvironmentResource;
import com.spectralogic.s3.common.rpc.tape.TapePartitionResource;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailureType;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoFailures;
import com.spectralogic.s3.common.rpc.tape.domain.BlobIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.BucketIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.S3ObjectsIoRequest;
import com.spectralogic.s3.common.rpc.tape.domain.TapeEnvironmentInformation;
import com.spectralogic.s3.common.testfrmwrk.MockCacheFilesystemDriver;
import com.spectralogic.s3.simulator.domain.SimDrive;
import com.spectralogic.s3.simulator.domain.SimTape;
import com.spectralogic.s3.simulator.state.SimStateManager;
import com.spectralogic.s3.simulator.state.simresource.api.SimulatorResource;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.net.rpc.client.RpcClient;
import com.spectralogic.util.net.rpc.client.RpcClientImpl;
import com.spectralogic.util.net.rpc.client.RpcProxyException;
import com.spectralogic.util.net.rpc.frmwrk.ConcurrentRequestExecutionPolicy;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;
import com.spectralogic.util.security.ChecksumType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static com.spectralogic.s3.simulator.Simulator.DEFAULT_DRIVE_SN;
import static com.spectralogic.s3.simulator.Simulator.DEFAULT_LIBRARY_SN;


@Tag("rpc-integration")
public final class SimulatorIntegration_Test
{
    @Test
    public void testTapeEnvironmentCommandsWorkAsExpected()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final Simulator simulator = new Simulator(Simulator.getTestConfig());
        final Thread t = new Thread( simulator, "Simulator" );
        t.start();
        
        final RpcClient rpcClient = new RpcClientImpl( "localhost", RpcServerPort.TAPE_BACKEND );
        
        new MockCacheFilesystemDriver( dbSupport, 1, 10000 ).shutdown();
        
        final SimStateManager stateManager = simulator.getStateManager( 20 );
        final TapeEnvironmentResource tapeEnvironmentResource = rpcClient.getRpcResource(
                TapeEnvironmentResource.class,
                null,
                ConcurrentRequestExecutionPolicy.CONCURRENT );

        final String partitionSerial = stateManager.getPartitions(DEFAULT_LIBRARY_SN).iterator().next().getSerialNumber();

        final TapePartitionResource tapePartitionResource = rpcClient.getRpcResource(
                TapePartitionResource.class,
                partitionSerial,
                ConcurrentRequestExecutionPolicy.SERIALIZED );
        Assertions.assertEquals(1, tapeEnvironmentResource.getTapeEnvironmentGenerationNumber()
        .get( Timeout.DEFAULT ).longValue(), "We've not yet discovered the tape environment and the sim just started, so it's changed.");
        final TapeEnvironmentInformation initialTapeEnv = 
                tapeEnvironmentResource.getTapeEnvironment().get( Timeout.DEFAULT );
        Assertions.assertEquals(1, initialTapeEnv.getLibraries().length, "Shoulda been a single library online.");
        Assertions.assertEquals(1, initialTapeEnv.getLibraries()[0].getPartitions().length, "Shoulda been a single partition online.");

        stateManager.updatePartition( partitionSerial, null, true );
        final TapeEnvironmentInformation defaultTapeEnv = 
                tapeEnvironmentResource.getTapeEnvironment().get( Timeout.DEFAULT );
        Assertions.assertEquals(1, defaultTapeEnv.getLibraries()[ 0 ].getPartitions().length, "Shoulda been the default partition online.");
        Assertions.assertEquals(1, defaultTapeEnv.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives().length, "Shoulda been the default partition online.");
        Assertions.assertEquals(1, defaultTapeEnv.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes().length, "Shoulda been the default partition online.");

        stateManager.addTape( newTape( partitionSerial, "a", stateManager.getStartTapeRange() + 1, 100 ) );
        stateManager.addTape( newTape( partitionSerial, "b", stateManager.getStartTapeRange() + 2, 100 ) );
        stateManager.addTape( newTape( partitionSerial, "c", stateManager.getStartTapeRange() + 3, 100 ) );
        stateManager.addTape( newTape( partitionSerial, "d", stateManager.getStartTapeRange() + 4, 100 ) );
        
        stateManager.addDrive( newDrive( partitionSerial, "1", 1 ) );
        stateManager.addDrive( newDrive( partitionSerial, "2", 2 ) );
        stateManager.addDrive( newDrive( partitionSerial, "3", 3 ) );
        stateManager.addDrive( newDrive( partitionSerial, "4", 4 ) );
        stateManager.updateDrive( "3", "Failed.", true );
        stateManager.updateDrive( "4", "Unknown.", false );
        
        final TapeEnvironmentInformation tapeEnv = 
                tapeEnvironmentResource.getTapeEnvironment().get( Timeout.DEFAULT );
        Assertions.assertEquals(1, tapeEnv.getLibraries()[ 0 ].getPartitions().length, "Shoulda been the modified partition online.");
        Assertions.assertEquals(4, tapeEnv.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives().length, "Shoulda been the modified partition online.");
        Assertions.assertEquals(5, tapeEnv.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes().length, "Shoulda been the modified partition online.");

        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                tapePartitionResource.move( stateManager.getStartTapeRange() + 1, stateManager.getStartTapeRange() + 2 ).get( Timeout.DEFAULT );
            }
        } );
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                tapePartitionResource.move( stateManager.getStartTapeRange() + 1, 99 ).get( Timeout.DEFAULT );
            }
        } );
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                tapePartitionResource.move( stateManager.getStartTapeRange() + 5, stateManager.getStartTapeRange() + 6 ).get( Timeout.DEFAULT );
            }
        } );
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                tapePartitionResource.move( 1, stateManager.getStartTapeRange() + 6 ).get( Timeout.DEFAULT );
            }
        } );
        
        final TapeDriveResource tapeDrive1 = rpcClient.getRpcResource(
                TapeDriveResource.class,
                "1",
                ConcurrentRequestExecutionPolicy.CONCURRENT );

        move( tapePartitionResource, stateManager.getStartTapeRange() + 1, 1 );
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                move( tapePartitionResource, 1, stateManager.getStartTapeRange() + 1 );
            }
        } );

        tapeDrive1.prepareForRemoval().get( Timeout.DEFAULT );
        move( tapePartitionResource, 1, stateManager.getStartTapeRange() + 1 );
        
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                tapePartitionResource.move( 1, stateManager.getStartTapeRange() + 1 ).get( Timeout.DEFAULT );
            }
        } );
        
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                tapePartitionResource.move( stateManager.getStartTapeRange() + 1, 5 ).get( Timeout.DEFAULT );
            }
        } );
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                tapePartitionResource.move( stateManager.getStartTapeRange() + 1, 4 ).get( Timeout.DEFAULT );
            }
        } );

        move( tapePartitionResource, stateManager.getStartTapeRange() + 1, 1 );
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                tapePartitionResource.move( stateManager.getStartTapeRange() + 2, 1 ).get( Timeout.DEFAULT );
            }
        } );

        stateManager.addDrive( newDrive( partitionSerial, "5", 5 ) );
        stateManager.updateDrive( "2", null, false );
        tapeEnvironmentResource.getTapeEnvironment().get( Timeout.DEFAULT );
        
        tapeDrive1.prepareForRemoval();
        move( tapePartitionResource, 1, stateManager.getStartTapeRange() + 1 );
        
        // Can't remove tape that isn't in the drive
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                tapePartitionResource.move( 1, stateManager.getStartTapeRange() + 9 ).get( Timeout.DEFAULT );
            }
        } );
        
        // Can't move from location that doesn't exist
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                tapePartitionResource.move( 9999, 1 ).get( Timeout.DEFAULT );
            }
        } );
        
        // Can't move to location that doesn't exist
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                tapePartitionResource.move( stateManager.getStartTapeRange() + 1, 9999 ).get( Timeout.DEFAULT );
            }
        } );
        
        rpcClient.shutdown();
        simulator.shutdown();
    }
    
    @Test
    public void testTapeDriveCommandsWorkAsExpected()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        final Simulator simulator = new Simulator(Simulator.getTestConfig());
        final Thread t = new Thread( simulator, "Simulator" );
        t.start();
        
        final RpcClient rpcClient = new RpcClientImpl( "localhost", RpcServerPort.TAPE_BACKEND );
        
        final SimStateManager stateManager = simulator.getStateManager( 20 );
        final String partitionSerial = stateManager.getPartitions(DEFAULT_LIBRARY_SN).iterator().next().getSerialNumber();
        final TapeEnvironmentResource tapeEnvironmentResource = rpcClient.getRpcResource( 
                TapeEnvironmentResource.class,
                null,
                ConcurrentRequestExecutionPolicy.CONCURRENT );
        
        stateManager.updatePartition( partitionSerial, null, true );
        
        stateManager.addTape( newTape( partitionSerial, "a", stateManager.getStartTapeRange() + 1, 20 ) );
        stateManager.addTape( newTape( partitionSerial, "b", stateManager.getStartTapeRange() + 2, 100 ) );
        stateManager.addTape( newTape( partitionSerial, "c", stateManager.getStartTapeRange() + 3, 100 ) );
        stateManager.addTape( newTape( partitionSerial, "d", stateManager.getStartTapeRange() + 4, 100 ) );
        
        stateManager.addDrive( newDrive( partitionSerial, "1", 1 ) );
        stateManager.addDrive( newDrive( partitionSerial, "2", 2 ) );
        stateManager.addDrive( newDrive( partitionSerial, "3", 3 ) );
        stateManager.addDrive( newDrive( partitionSerial, "4", 4 ) );
        stateManager.updateDrive( "3", "Failed.", true );
        stateManager.updateDrive( "4", null, false );
       
        new MockCacheFilesystemDriver( dbSupport, 1, 10000 ).shutdown();
        
        final TapeEnvironmentInformation tapeEnv = 
                tapeEnvironmentResource.getTapeEnvironment().get( Timeout.DEFAULT );
        Assertions.assertEquals(1, tapeEnv.getLibraries()[ 0 ].getPartitions().length, "Shoulda been the modified partition online.");
        Assertions.assertEquals(4, tapeEnv.getLibraries()[ 0 ].getPartitions()[ 0 ].getDrives().length, "Shoulda been the modified partition online.");
        Assertions.assertEquals(5, tapeEnv.getLibraries()[ 0 ].getPartitions()[ 0 ].getTapes().length, "Shoulda been the modified partition online.");
        
        final String psn = tapeEnv.getLibraries()[ 0 ].getPartitions()[ 0 ].getSerialNumber();
        final TapePartitionResource tapePartitionResource = rpcClient.getRpcResource( 
                TapePartitionResource.class,
                psn,
                ConcurrentRequestExecutionPolicy.SERIALIZED );
        move( tapePartitionResource, stateManager.getStartTapeRange() + 1, 1 );
        move( tapePartitionResource, stateManager.getStartTapeRange() + 2, 2 );

        final TapeDriveResource tapeDriveDefault = rpcClient.getRpcResource(
                TapeDriveResource.class, 
                DEFAULT_DRIVE_SN + "0",
                ConcurrentRequestExecutionPolicy.CONCURRENT );
        final TapeDriveResource tapeDrive1 = rpcClient.getRpcResource(
                TapeDriveResource.class,
                "1",
                ConcurrentRequestExecutionPolicy.CONCURRENT );
        final TapeDriveResource tapeDrive2 = rpcClient.getRpcResource(
                TapeDriveResource.class, 
                "2",
                ConcurrentRequestExecutionPolicy.CONCURRENT );
        final TapeDriveResource tapeDrive3 = rpcClient.getRpcResource(
                TapeDriveResource.class, 
                "3",
                ConcurrentRequestExecutionPolicy.CONCURRENT );
        final TapeDriveResource tapeDrive4 = rpcClient.getRpcResource(
                TapeDriveResource.class, 
                "4",
                ConcurrentRequestExecutionPolicy.CONCURRENT );
        
        Assertions.assertTrue(tapeDriveDefault.isServiceable(), "Backend shoulda initialized tape drive resources it was instructed to initialize.");
        Assertions.assertTrue(tapeDrive1.isServiceable(), "Backend shoulda initialized tape drive resources it was instructed to initialize.");
        Assertions.assertTrue(tapeDrive2.isServiceable(), "Backend shoulda initialized tape drive resources it was instructed to initialize.");
        Assertions.assertTrue(tapeDrive3.isServiceable(), "Backend shoulda initialized tape drive resources it was instructed to initialize.");
        Assertions.assertFalse(tapeDrive4.isServiceable(), "Backend shoulda initialized tape drive resources it was instructed to initialize.");

        tapeDrive1.format( false, null ).get( Timeout.DEFAULT );

        // Can't read what's not on tape
        final BlobIoFailures cantReadFailures = tapeDrive1.readData(
                createObjects( createObject( createBlob( 1, -1 ) ) ) ).get( Timeout.DEFAULT );
        Assertions.assertEquals(1, cantReadFailures.getFailures().length, "Shoulda returned a single failure.");
        Assertions.assertEquals(BlobIoFailureType.DOES_NOT_EXIST, cantReadFailures.getFailures()[ 0 ].getFailure(), "Shoulda returned a single failure.");
        
        // Can't write what's not in cache
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                tapeDrive1.writeData(
                        LtfsFileNamingMode.values()[ 0 ],
                        createObjects( createObject( createBlob( 1, -1 ) ) ) ).get( Timeout.DEFAULT );
            }
        } );

        tapeDrive1.writeData( 
                LtfsFileNamingMode.values()[ 0 ],
                createObjects( createObject( createBlob( 1, 14 ) ) ) ).get( Timeout.DEFAULT );
        
        // Not enough space on tape
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                tapeDrive1.writeData(
                        LtfsFileNamingMode.values()[ 0 ],
                        createObjects( createObject( createBlob( 2, 100 ) ) ) ).get( Timeout.DEFAULT );
            }
        } );
        
        // It's bad practice to not quiesce after a write - see if sim catches that
        TestUtil.assertThrows( null, RpcProxyException.class, new BlastContainer()
        {
            public void test()
            {
                tapeDrive1.writeData( 
                        LtfsFileNamingMode.values()[ 0 ],
                        createObjects( createObject( createBlob( 3, 2 ) ) ) ).get( Timeout.DEFAULT );
            }
        } );
        
        tapeDrive1.quiesce().get( Timeout.DEFAULT );
        tapeDrive1.writeData( 
                LtfsFileNamingMode.values()[ 0 ],
                createObjects( createObject( createBlob( 5, 2 ) ) ) ).get( Timeout.DEFAULT );
        
        tapeDrive2.format(false, null ).get( Timeout.DEFAULT );
        tapeDrive2.writeData( 
                LtfsFileNamingMode.values()[ 0 ],
                createObjects( createObject( createBlob( 4, 50 ) ) ) ).get( Timeout.DEFAULT );
        
        rpcClient.shutdown();
        simulator.shutdown();
    }
    
    
    private SimDrive newDrive( final String partitionSerialNumber, final String sn, final int elementAddress )
    {
        final SimDrive retval = BeanFactory.newBean( SimDrive.class );
        retval.setPartitionSerialNumber( partitionSerialNumber );
        retval.setElementAddress( elementAddress );
        retval.setType( TapeDriveType.LTO6 );
        retval.setSerialNumber( sn );
        retval.setMfgSerialNumber( sn );
        return retval;
    }
    
    
    private SimTape newTape(
            final String partitionSerialNumber,
            final String barCode,
            final int elementAddress, 
            final int capacity )
    {
        final SimTape retval = BeanFactory.newBean( SimTape.class );
        retval.setElementAddress( elementAddress );
        retval.setPartitionSerialNumber( partitionSerialNumber );
        retval.setTotalRawCapacity( capacity );
        retval.setAvailableRawCapacity( capacity );
        retval.setType( TapeType.LTO6 );
        retval.setBarCode( barCode );
        return retval;
    }
    
    
    private void move( 
            final TapePartitionResource tapePartitionResource, 
            final int srcElementAddress,
            final int destElementAddress )
    {
        int maxTries = 10;
        while ( --maxTries > 0 )
        {
            try
            {
                tapePartitionResource.move( srcElementAddress, destElementAddress )
                    .get( Timeout.DEFAULT );
                return;
            }
            catch ( final RpcProxyException ex )
            {
                Validations.verifyNotNull( "Shut up CodePro.", ex );
            }
        }
        
        tapePartitionResource.move( srcElementAddress, destElementAddress ).get( Timeout.DEFAULT );
    }
    
    
    private BlobIoRequest createBlob( 
            final int objectNumber,
            final int length )
    {
        final File file;
        try
        {
            file = File.createTempFile( getClass().getSimpleName(), "num" + objectNumber );
            if ( 0 <= length )
            {
                file.createNewFile();
                final FileOutputStream out = new FileOutputStream( file );
                for ( int i = 0; i < length; ++i )
                {
                    out.write( 2 );
                }
                out.close();
                file.deleteOnExit();
            }
            else
            {
                file.delete();
            }
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
        
        final BlobIoRequest retval = BeanFactory.newBean( BlobIoRequest.class );
        final String fullPath = file.getAbsolutePath();
        retval.setFileName( fullPath.substring( fullPath.lastIndexOf( Platform.FILE_SEPARATOR ) + 1 ) );
        retval.setOffset( objectNumber );
        retval.setLength( length );
        retval.setId( UUID.randomUUID() );
        retval.setChecksum( "checksumvalue" );
        retval.setChecksumType( ChecksumType.MD5 );
        return retval;
    }
    
    
    private S3ObjectIoRequest createObject( final BlobIoRequest ... blobs )
    {
        final S3ObjectIoRequest retval = BeanFactory.newBean( S3ObjectIoRequest.class );
        retval.setObjectName( "object" );
        retval.setBlobs( blobs );
        retval.setId( UUID.randomUUID() );
        return retval;
    }
    
    
    private S3ObjectsIoRequest createObjects( final S3ObjectIoRequest ... objects )
    {
        final S3ObjectsIoRequest retval = BeanFactory.newBean( S3ObjectsIoRequest.class );
        retval.setBuckets( new BucketIoRequest [] { BeanFactory.newBean( BucketIoRequest.class ) } );
        retval.getBuckets()[ 0 ].setBucketName( "bucket" );
        retval.getBuckets()[ 0 ].setObjects( objects );
        retval.setCacheRootPath( TEMP_DIR );
        return retval;
    }
    
    
    private final static String TEMP_DIR;
    static
    {
        String tempDir = System.getProperty( "java.io.tmpdir" );
        if ( !tempDir.endsWith( Platform.FILE_SEPARATOR ) )
        {
            tempDir += Platform.FILE_SEPARATOR;
        }
        TEMP_DIR = tempDir;
    }
}
