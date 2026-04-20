/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;

import java.util.Locale;

public final class ModifyTapePartitionRequestHandler_Test 
{
    @Test
    public void testtestModifyTapePartitionNameNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, "testtp" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Name", "name-testtp" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() )
                        .addParameter( NameObservable.NAME, "foo_bar" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testtestModifyTapePartitionQuiescedStateOnlyAllowedForValidStateTransitions()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, "testtp" );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() )
                    .addParameter( 
                            TapePartition.QUIESCED,
                            Quiesced.NO.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() )
                    .addParameter( 
                            TapePartition.QUIESCED,
                            Quiesced.YES.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() )
                    .addParameter( 
                            TapePartition.QUIESCED,
                            Quiesced.PENDING.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "PENDING" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() )
                    .addParameter( 
                            TapePartition.QUIESCED,
                            Quiesced.YES.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() )
                    .addParameter( 
                            TapePartition.QUIESCED,
                            Quiesced.NO.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Quiesced", "NO" );
    }

    @Test
    public void testtestModifyTapePartitionEnableAutoQuiesce()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, "testtp" );

        // verify auto-quiesce is enabled
        assertTrue( mockDaoDriver.retrieve( partition ).isAutoQuiesceEnabled() );

        // enable auto-quiesce
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() )
                    .addParameter( TapePartition.AUTO_QUIESCE_ENABLED, "true" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Name", "name-testtp" );
        assertTrue( mockDaoDriver.retrieve( partition ).isAutoQuiesceEnabled() );

        // disable auto-quiesce
        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() )
                .addParameter( TapePartition.AUTO_QUIESCE_ENABLED, "false" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Name", "name-testtp" );
        assertFalse( mockDaoDriver.retrieve( partition ).isAutoQuiesceEnabled() );
    }

    @Test
    public void testtestModifyBothReserveMinimumsNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, "testtp" );
        mockDaoDriver.createTapeDrive(partition.getId(), "drive1");
        mockDaoDriver.createTapeDrive(partition.getId(), "drive2");

        //try to set both
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() )
                .addParameter( TapePartition.MINIMUM_READ_RESERVED_DRIVES, "1" )
                .addParameter( TapePartition.MINIMUM_WRITE_RESERVED_DRIVES, "1" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        // try to set one
        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() )
                .addParameter( TapePartition.MINIMUM_READ_RESERVED_DRIVES, "1" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        assertEquals( mockDaoDriver.retrieve( partition ).getMinimumReadReservedDrives(), 1 );

        // try to set the other with one already set
        driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() )
                .addParameter( TapePartition.MINIMUM_WRITE_RESERVED_DRIVES, "1" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        assertEquals( mockDaoDriver.retrieve( partition ).getMinimumWriteReservedDrives(), 0 );
    }

    @Test
    public void testtestModifyTapePartitionSetDriveIdleTimeoutInMinutes()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, "testtp" );

        // verify drive idle timeout is default value
        assertEquals( Integer.valueOf(15), mockDaoDriver.retrieve( partition ).getDriveIdleTimeoutInMinutes() );

        // set a different idle timeout
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() )
                .addParameter( TapePartition.DRIVE_IDLE_TIMEOUT_IN_MINUTES, "7" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientXPathEquals( "/Data/Name", "name-testtp" );
        assertEquals( Integer.valueOf(7), mockDaoDriver.retrieve( partition ).getDriveIdleTimeoutInMinutes() );
    }

    @Test
    public void testtestModifyTapePartitionSetDriveIdleTimeoutInMinutesNotAllowNegativeValue()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, "testtp" );

        // verify drive idle timeout is default value
        assertEquals(Integer.valueOf(15), mockDaoDriver.retrieve( partition ).getDriveIdleTimeoutInMinutes() );

        // set a different idle timeout
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() )
                .addParameter( TapePartition.DRIVE_IDLE_TIMEOUT_IN_MINUTES, "-1" );

        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        assertEquals( Integer.valueOf(15), mockDaoDriver.retrieve( partition ).getDriveIdleTimeoutInMinutes() );
    }

    @Test
    public void testtestModifyTapePartitionDisableDriveIdleTimeoutInMinutes()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, "testtp" );

        // verify drive idle timeout is default value
        assertEquals( Integer.valueOf(15), mockDaoDriver.retrieve( partition ).getDriveIdleTimeoutInMinutes() );

        // disable the drive idle timeout
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/tape_partition/" + partition.getId() )
                .addParameter( TapePartition.DRIVE_IDLE_TIMEOUT_IN_MINUTES, null );

        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertNull(mockDaoDriver.retrieve( partition ).getDriveIdleTimeoutInMinutes());
    }
}
