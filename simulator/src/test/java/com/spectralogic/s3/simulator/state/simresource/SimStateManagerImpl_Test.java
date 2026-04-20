/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator.state.simresource;

import com.spectralogic.s3.common.dao.domain.tape.ElementAddressType;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.rpc.tape.domain.ElementAddressBlockInformation;
import com.spectralogic.s3.simulator.Simulator;
import com.spectralogic.s3.simulator.domain.SimDrive;
import com.spectralogic.s3.simulator.domain.SimLibrary;
import com.spectralogic.s3.simulator.domain.SimPartition;
import com.spectralogic.s3.simulator.domain.SimTape;
import com.spectralogic.s3.simulator.state.SimStateManager;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.server.RpcServer;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public final class SimStateManagerImpl_Test
{
    @Test
    public void testConstructorNullRpcServerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new SimStateManagerImpl( null );
            }
        } );
    }
    
    @Test
    public void testAddLibraryNullLibraryNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addLibrary( null );
            }
        } );
    }
    
    @Test
    public void testAddLibraryNullSerialNumberNotAllowed()
    {
        final SimLibrary library = BeanFactory.newBean( SimLibrary.class );
        
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addLibrary( library );
            }
        } );
    }
    
    @Test
    public void testAddPartitionNullPartitionNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addPartition( null );
            }
        } );
    }
    
    @Test
    public void testAddPartitionNullSerialNumberNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        final SimPartition partition = BeanFactory.newBean( SimPartition.class );
        partition.setLibrarySerialNumber( "lsn" );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addPartition( partition );
            }
        } );
    }
    
    @Test
    public void testAddPartitionNullLibrarySerialNumberNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        final SimPartition partition = BeanFactory.newBean( SimPartition.class );
        partition.setSerialNumber( "a" );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addPartition( partition );
            }
        } );
    }
    
    @Test
    public void testPartitionManagementWorks()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig());
        assertEquals(
                0,
                manager.getPartitions( null ).size(),
                "Should notta been any partitions to start off."
                 );
        final SimPartition partition = BeanFactory.newBean( SimPartition.class );
        partition.setLibrarySerialNumber( "lsn" );
        partition.setSerialNumber( "a" );
        partition.setOnline( false );
        manager.hasChanged();
        manager.addPartition( partition );
        assertTrue(
                manager.hasChanged(),
                "Shoulda reported a change.");
        assertEquals(
                0,
                manager.getPartitions( null ).size(),
                "Should notta included partition that is offline."
                );
        
        manager.updatePartition( "a", null, true );
        assertEquals(
                1,
                manager.getPartitions( null ).size(),
                "Shoulda been a partition created."
               );
        assertEquals(
                partition,
                manager.getPartition( "a" ),
                "Shoulda been a partition created."
                 );
        assertEquals(
                null,
                manager.getPartition( "b" ),
                "Shoulda been a partition created."
                );
        
        manager.updatePartition( "a", null, false );
        assertEquals(
                0,
                manager.getPartitions( null ).size(),
                "Should notta included partition that is offline."
                );
        assertEquals(
                null,
                manager.getPartition( "a" ),
                "Should notta reported partition that is offline."
                 );
        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addPartition( partition );
            }
        } );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.updatePartition( "b", null, false );
            }
        } );
    }
    
    @Test
    public void testAddDriveNullDriveNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addDrive( null );
            }
        } );
    }
    
    @Test
    public void testAddDriveNullPartitionSerialNumberNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        
        final SimDrive drive = BeanFactory.newBean( SimDrive.class );
        drive.setSerialNumber( "drive1" );
        drive.setMfgSerialNumber( "drive1" );
        drive.setType( TapeDriveType.LTO6 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addDrive( drive );
            }
        } );
    }
    
    @Test
    public void testAddDriveNullSerialNumberNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        
        final SimDrive drive = BeanFactory.newBean( SimDrive.class );
        drive.setPartitionSerialNumber( "a" );
        drive.setMfgSerialNumber( "drive" );
        drive.setType( TapeDriveType.LTO6 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addDrive( drive );
            }
        } );
    }
    
    @Test
    public void testAddDriveNullTypeNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        
        final SimDrive drive = BeanFactory.newBean( SimDrive.class );
        drive.setPartitionSerialNumber( "a" );
        drive.setSerialNumber( "drive1" );
        drive.setMfgSerialNumber( "drive1" );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addDrive( drive );
            }
        } );
    }
    
    @Test
    public void testDriveManagementWorks()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        manager.addPartition( createPartition( "a" ) );
        manager.addPartition( createPartition( "b" ) );
        assertEquals(
                0,
                manager.getDrives( "a" ).size(),
                "Should notta reported any drives yet."
                 );
        
        final SimDrive drive = BeanFactory.newBean( SimDrive.class );
        drive.setPartitionSerialNumber( "a" );
        drive.setSerialNumber( "drive1" );
        drive.setMfgSerialNumber( "drive1" );
        drive.setType( TapeDriveType.LTO6 );
        manager.hasChanged();
        manager.addDrive( drive ); 
        assertTrue(
                manager.hasChanged(),
                "Shoulda reported a change."
                 );
        assertEquals(
                1,
                manager.getDrives( "a" ).size(),
                "Shoulda reported created drive."
                 );
        
        manager.updateDrive( "drive1", null, false );
        assertEquals(
                0,
                manager.getDrives( null ).size(),
                "Should notta reported offline drive."
               );
        
        manager.updateDrive( "drive1", null, true );
        assertEquals(
                1,
                manager.getDrives( null ).size(),
                "Shoulda reported erred drive."
                 );
        assertEquals(
                0,
                manager.getDrives( "invalid" ).size(),
                "Should notta reported drive since not in the invalid partition."
                 );
        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addDrive( drive );
            }
        } );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.updateDrive( "invalid", null, true );
            }
        } );
        
        final SimDrive drive2 = BeanFactory.newBean( SimDrive.class );
        drive2.setPartitionSerialNumber( "a" );
        drive2.setSerialNumber( "drive2" );
        drive2.setMfgSerialNumber( "drive2" );
        drive2.setType( TapeDriveType.LTO6 );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addDrive( drive2 );
            }
        } );

        drive.setElementAddress( 1 );
        manager.addDrive( drive2 );
        assertEquals(
                2,
                manager.getDrives( null ).size(),
                "Shoulda reported all drives."
                 );

        final SimDrive drive3 = BeanFactory.newBean( SimDrive.class );
        drive3.setPartitionSerialNumber( "b" );
        drive3.setSerialNumber( "drive3" );
        drive3.setMfgSerialNumber( "drive3" );
        drive3.setType( TapeDriveType.LTO6 );
        manager.addDrive( drive3 );
        assertEquals(
                3,
                manager.getDrives( null ).size(),
                "Shoulda reported all drives."
                 );
        assertEquals(
                1,
                manager.getDrives( "b" ).size(),
                "Shoulda reported all drives in partition."
                 );
        assertEquals(
                drive2,
                manager.getDrive( "drive2" ),
                "Shoulda reported drive by serial number."
                 );
        assertEquals(
                null,
                manager.getDrive( "dafsdasdads" ),
                "Shoulda reported null since no drive matching serial number."
                 );
        assertEquals(
                drive2,
                manager.getDrive( drive2.getPartitionSerialNumber(), drive2.getElementAddress() ),
                "Shoulda reported drive by element address."
                 );
        assertEquals(
                null,
                manager.getDrive( drive2.getPartitionSerialNumber(), 999 ),
                "Shoulda reported null since no drive matching element address."
                 );
        assertEquals(
                null,
                manager.getDrive( "asufkhsa", drive2.getElementAddress() ),
                "Shoulda reported null since no drive matching element address."
                 );

        final SimDrive drive4 = BeanFactory.newBean( SimDrive.class );
        drive4.setPartitionSerialNumber( "c" );
        drive4.setSerialNumber( "drive4" );
        drive4.setMfgSerialNumber( "drive4" );
        drive4.setType( TapeDriveType.LTO6 );
        manager.addDrive( drive4 );
        assertEquals(
                3,
                manager.getDrives( null ).size(),
                "Should notta reported drive that doesn't belong to a partition."
                );

        final SimDrive drive5 = BeanFactory.newBean( SimDrive.class );
        drive5.setPartitionSerialNumber( "c" );
        drive5.setSerialNumber( "drive5" );
        drive5.setMfgSerialNumber( "drive5" );
        drive5.setType( TapeDriveType.LTO6 );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addDrive( drive5 );
            }
        } );
    }
    
    @Test
    public void testAddTapeNullPartitionNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addTape( null );
            }
        } );
    }
    
    @Test
    public void testAddTapeNullBarCodeNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        final SimTape tape = BeanFactory.newBean( SimTape.class );
        tape.setPartitionSerialNumber( "a" );
        tape.setAvailableRawCapacity( 1 );
        tape.setTotalRawCapacity( 1 );
        tape.setType( TapeType.values()[ 0 ] );
        tape.setElementAddress( 1 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addTape( tape );
            }
        } );
    }
    
    @Test
    public void testAddTapeNullPartitionSerialNumberNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        final SimTape tape = BeanFactory.newBean( SimTape.class );
        tape.setBarCode( "t1" );
        tape.setAvailableRawCapacity( 1 );
        tape.setTotalRawCapacity( 1 );
        tape.setType( TapeType.values()[ 0 ] );
        tape.setElementAddress( 1 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addTape( tape );
            }
        } );
    }
    
    @Test
    public void testAddTapeZeroTotalRawCapacityNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        final SimTape tape = BeanFactory.newBean( SimTape.class );
        tape.setBarCode( "t1" );
        tape.setPartitionSerialNumber( "a" );
        tape.setType( TapeType.values()[ 0 ] );
        tape.setElementAddress( 1 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addTape( tape );
            }
        } );
    }
    
    @Test
    public void testAddTapeAvailableCapacityGreaterThanTotalCapacityNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        final SimTape tape = BeanFactory.newBean( SimTape.class );
        tape.setBarCode( "t1" );
        tape.setPartitionSerialNumber( "a" );
        tape.setAvailableRawCapacity( 2 );
        tape.setTotalRawCapacity( 1 );
        tape.setType( TapeType.values()[ 0 ] );
        tape.setElementAddress( 1 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addTape( tape );
            }
        } );
    }
    
    @Test
    public void testAddTapeNullTypeNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        final SimTape tape = BeanFactory.newBean( SimTape.class );
        tape.setBarCode( "t1" );
        tape.setPartitionSerialNumber( "a" );
        tape.setAvailableRawCapacity( 1 );
        tape.setTotalRawCapacity( 1 );
        tape.setElementAddress( 1 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addTape( tape );
            }
        } );
    }
    
    @Test
    public void testTapeManagementWorks()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );
        assertEquals(
                0,
                manager.getPartitions( null ).size(),
                "Should notta been any tapes to start off."
               );

        final SimPartition partition = BeanFactory.newBean( SimPartition.class );
        partition.setLibrarySerialNumber( "lsn" );
        partition.setSerialNumber( "a" );
        manager.addPartition( partition );
        
        final SimTape tape = BeanFactory.newBean( SimTape.class );
        tape.setBarCode( "t1" );
        tape.setPartitionSerialNumber( partition.getSerialNumber() );
        tape.setAvailableRawCapacity( 1 );
        tape.setTotalRawCapacity( 1 );
        tape.setElementAddress( 1 );
        tape.setType( TapeType.values()[ 0 ] );
        tape.setOnline( false );
        manager.hasChanged();
        
        final String serialNumber = manager.addTape( tape ).getWithoutBlocking().getHardwareSerialNumber();
        assertTrue(
                manager.hasChanged(),
                "Shoulda reported a change.");
        assertEquals(
                0,
                manager.getTapes( tape.getPartitionSerialNumber() ).size(),
                "Should notta included tape that is offline."
                );
        
        manager.updateTape( serialNumber, true );
        assertEquals(
                1,
                manager.getTapes( tape.getPartitionSerialNumber() ).size(),
                "Shoulda been a tape created."
               );
        
        manager.updateTape( serialNumber, false );
        assertEquals(
                0,
                manager.getTapes( tape.getPartitionSerialNumber() ).size(),
                "Should notta included tape that is offline."
                );
        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addTape( tape.setHardwareSerialNumber( serialNumber ) );
            }
        } );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.updateTape( "nonexistant", false );
            }
        } );
        
        final SimTape tape2 = BeanFactory.newBean( SimTape.class );
        tape2.setBarCode( "t2" );
        tape2.setPartitionSerialNumber( partition.getSerialNumber() );
        tape2.setAvailableRawCapacity( 1 );
        tape2.setTotalRawCapacity( 1 );
        tape2.setElementAddress( 1 );
        tape2.setType( TapeType.values()[ 0 ] );
        manager.hasChanged();
        
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addTape( tape2 );
            }
        } );
        
        tape2.setElementAddress( 2 );
        manager.addTape( tape2 );
        
        manager.addPartition( createPartition( "sec" ) );
        
        final SimTape tape3 = BeanFactory.newBean( SimTape.class );
        tape3.setBarCode( "t3" );
        tape3.setPartitionSerialNumber( "sec" );
        tape3.setAvailableRawCapacity( 1 );
        tape3.setTotalRawCapacity( 1 );
        tape3.setElementAddress( 1 );
        tape3.setType( TapeType.values()[ 0 ] );
        tape3.setElementAddress( 2 );
        manager.addTape( tape3 );
        assertEquals(
                2,
                manager.getTapes( null ).size(),
                "Shoulda included all tapes not offline."
                );
        
        final SimTape tape4 = BeanFactory.newBean( SimTape.class );
        tape4.setBarCode( "t4" );
        tape4.setPartitionSerialNumber( "sec" );
        tape4.setAvailableRawCapacity( 1 );
        tape4.setTotalRawCapacity( 1 );
        tape4.setElementAddress( 1 );
        tape4.setType( TapeType.values()[ 0 ] );
        tape4.setElementAddress( 2 );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addTape( tape4 );
            }
        } );
        tape4.setElementAddress( 1 );
        manager.addTape( tape4 );
        assertEquals(
                3,
                manager.getTapes( null ).size(),
                "Shoulda included all tapes not offline."
                 );
        
        final SimTape tape5 = BeanFactory.newBean( SimTape.class );
        tape5.setBarCode( "t5" );
        tape5.setPartitionSerialNumber( "tres" );
        tape5.setAvailableRawCapacity( 1 );
        tape5.setTotalRawCapacity( 1 );
        tape5.setElementAddress( 1 );
        tape5.setType( TapeType.values()[ 0 ] );
        tape5.setElementAddress( 2 );
        manager.addTape( tape5 );
        assertEquals(
                3,
                manager.getTapes( null ).size(),
                "Should notta included tape for partition that doesn't exist."
               );
        manager.updatePartition( "sec", null, false );
        assertEquals(
                1,
                manager.getTapes( null ).size(),
                "Should notta included tape for partition that is offline."
                );
        assertEquals(
                1,
                manager.getTapes( "a" ).size(),
                "Shoulda returned tape."
                );
        assertEquals(
                0,
                manager.getTapes( "tres" ).size(),
                "Shoulda returned tape."
                 );
        assertEquals(
                tape2,
                manager.getTape( tape2.getHardwareSerialNumber() ),
                "Shoulda returned tape."
                );
        assertEquals(
                null,
                manager.getTape( tape5.getHardwareSerialNumber() ),
                "Should notta returned tape."
                );
        assertEquals(
                tape2,
                manager.getTape( tape2.getPartitionSerialNumber(), 2 ),
                "Shoulda returned tape."
                );
        assertEquals(
                null,
                manager.getTape( tape2.getPartitionSerialNumber(), 4 ),
                "Should notta returned tape."
                 );
        assertEquals(
                null,
                manager.getTape( tape5.getPartitionSerialNumber(), 2 ),
                "Should notta returned tape."
                 );
    }
    
    @Test
    public void testSetElementAddressBlocksNullTapePartitionSerialNumberNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );

        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.setElementAddressBlocks( null, ElementAddressType.values()[ 0 ], null );
            }
        } );
    }
    
    @Test
    public void testSetElementAddressBlocksNullElementAddressTypeNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );

        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.setElementAddressBlocks( "sn", null, null );
            }
        } );
    }
    
    @Test
    public void testAddElementAddressBlocksNullTapePartitionSerialNumberNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );

        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addElementAddressBlock( null, ElementAddressType.values()[ 0 ], 1, 2 );
            }
        } );
    }
    
    @Test
    public void testAddElementAddressBlocksNullTypeNotAllowed()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );

        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addElementAddressBlock( "sn", null, 1, 2 );
            }
        } );
    }
    
    @Test
    public void testElementAddressBlockManagementWorks()
    {
        final SimStateManager manager = new SimStateManagerImpl( getMockRpcServer(), Simulator.getEmptyConfig() );

        assertEquals(
                null,
                manager.getElementAddressBlocks( "a" ),
                "Shoulda returned element address blocks."
                 );
        manager.addElementAddressBlock( "a", ElementAddressType.values()[ 0 ], 1, 2 );
        assertEquals(
                1,
                manager.getElementAddressBlocks( "a" ).size(),
                "Shoulda returned element address blocks."
                );
        manager.addElementAddressBlock( "a", ElementAddressType.values()[ 1 ], 3, 4 );
        assertEquals(
                2,
                manager.getElementAddressBlocks( "a" ).size(),
                "Shoulda returned element address blocks."
              );
        manager.addElementAddressBlock( "a", ElementAddressType.values()[ 1 ], 5, 5 );
        assertEquals(
                3,
                manager.getElementAddressBlocks( "a" ).size(),
                "Shoulda returned element address blocks."
                 );
        manager.setElementAddressBlocks(
                "a", 
                ElementAddressType.values()[ 1 ], 
                new ElementAddressBlockInformation [] 
                        { createBlock( ElementAddressType.values()[ 1 ], 6, 7 ) } );
        assertEquals(
                2,
                manager.getElementAddressBlocks( "a" ).size(),
                "Shoulda returned element address blocks."
                );
        
        manager.addElementAddressBlock( "b", ElementAddressType.values()[ 0 ], 1, 2 );
        assertEquals(
                2,
                manager.getElementAddressBlocks( "a" ).size(),
                "Shoulda returned element address blocks."
                );
        assertEquals(
                1,
                manager.getElementAddressBlocks( "b" ).size(),
                "Shoulda returned element address blocks."
                 );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addElementAddressBlock( "a", ElementAddressType.values()[ 0 ], 1, 2 );
            }
        } );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.getElementAddressBlocks( "a" );
            }
        } );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.getElementAddressBlocks( "b" );
            }
        } );

        manager.setElementAddressBlocks(
                "a", 
                ElementAddressType.values()[ 0 ], 
                new ElementAddressBlockInformation [] 
                        { createBlock( ElementAddressType.values()[ 0 ], 1, 2 ) } );
        assertEquals(
                1,
                manager.getElementAddressBlocks( "b" ).size(),
                "Shoulda returned element address blocks."
                );

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.addElementAddressBlock( "a", ElementAddressType.values()[ 0 ], 3, 2 );
            }
        } );
        manager.setElementAddressBlocks(
                "a", 
                ElementAddressType.values()[ 0 ], 
                new ElementAddressBlockInformation [] 
                        { createBlock( ElementAddressType.values()[ 0 ], 1, 2 ) } );
        assertEquals(
                1,
                manager.getElementAddressBlocks( "b" ).size(),
                "Shoulda returned element address blocks."
                );

        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                manager.setElementAddressBlocks(
                        "a", 
                        ElementAddressType.values()[ 0 ], 
                        new ElementAddressBlockInformation [] 
                                { createBlock( ElementAddressType.values()[ 1 ], 1, 2 ) } );
            }
        } );
        manager.setElementAddressBlocks(
                "a", 
                ElementAddressType.values()[ 0 ], 
                new ElementAddressBlockInformation [] 
                        { createBlock( ElementAddressType.values()[ 0 ], 1, 2 ) } );
        assertEquals(
                1,
                manager.getElementAddressBlocks( "b" ).size(),
                "Shoulda returned element address blocks."
                );
        
        manager.setElementAddressBlocks(
                "c", 
                ElementAddressType.values()[ 0 ], 
                new ElementAddressBlockInformation [] 
                        { createBlock( ElementAddressType.values()[ 0 ], 1, 2 ) } );
        assertEquals(
                1,
                manager.getElementAddressBlocks( "b" ).size(),
                "Shoulda returned element address blocks."
                 );
    }
    
    
    private SimPartition createPartition( final String partitionSerialNumber )
    {
        final SimPartition partition = BeanFactory.newBean( SimPartition.class );
        partition.setSerialNumber( partitionSerialNumber );
        partition.setLibrarySerialNumber( "lsn" );
        partition.setOnline( true );
        return partition;
    }
    
    
    private ElementAddressBlockInformation createBlock( 
            final ElementAddressType type, final int start, final int end )
    {
        final ElementAddressBlockInformation retval = 
                BeanFactory.newBean( ElementAddressBlockInformation.class );
        retval.setType( type );
        retval.setStartAddress( start );
        retval.setEndAddress( end );
        return retval;
    }
    
    
    private RpcServer getMockRpcServer()
    {
        return InterfaceProxyFactory.getProxy( RpcServer.class, null );
    }
}
