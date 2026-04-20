package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.tape.*;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class MarkTapeForCompactionRequestHandler_Test 
{
    // Verifies that a tape in the normal state can be manually marked for compaction.
    @Test
    public void testtestMarkTapeForCompactionSuccessful()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );

        final TapePartition tapePartition = mockDaoDriver.createTapePartition( null, null,
                TapeDriveType.LTO6, true );

        final Tape tape = mockDaoDriver.createTape( tapePartition.getId(), TapeState.NORMAL );

        final TapeService tapeService =
                support.getDatabaseSupport().getServiceManager().getService( TapeService.class );

        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "Test Domain" );
        final StorageDomainMember domainMember = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO6 );
        tape.setStorageDomainMemberId( domainMember.getId() );
        tapeService.update( tape, Tape.STORAGE_DOMAIN_MEMBER_ID );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.TAPE + "/" + tape.getId() )
                .addParameter( "operation", RestOperationType.MARK_FOR_COMPACTION.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(TapeState.AUTO_COMPACTION_IN_PROGRESS, tapeService.attain( tape.getId() ).getState(), "Should have updated state.");
    }


    // Verifies that a tape that is already marked for compaction remains unaffected by
    // marking it for compaction again.
    @Test
    public void testtestMarkTapeForCompactionWhenAlreadyMarkedSuccessful()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );

        final TapePartition tapePartition = mockDaoDriver.createTapePartition( null, null,
                TapeDriveType.LTO6, true );

        final Tape tape = mockDaoDriver.createTape( tapePartition.getId(), TapeState.AUTO_COMPACTION_IN_PROGRESS );

        final TapeService tapeService =
                support.getDatabaseSupport().getServiceManager().getService( TapeService.class );

        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "Test Domain" );
        final StorageDomainMember domainMember = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO6 );
        tape.setStorageDomainMemberId( domainMember.getId() );
        tapeService.update( tape, Tape.STORAGE_DOMAIN_MEMBER_ID );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.TAPE + "/" + tape.getId() )
                .addParameter( "operation", RestOperationType.MARK_FOR_COMPACTION.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(TapeState.AUTO_COMPACTION_IN_PROGRESS, tapeService.attain( tape.getId() ).getState(), "State should remain unchanged.");
    }

    // This is a negative test that verifies a 409 is returned if the user attempts to mark a
    // tape for compaction when the tape's state is not NORMAL.
    @Test
    public void testtestMarkTapeForCompactionWhenNotNormalStateNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );

        final TapePartition tapePartition = mockDaoDriver.createTapePartition( null, null,
                TapeDriveType.LTO6, true );

        final Tape tape = mockDaoDriver.createTape( tapePartition.getId(), TapeState.FORMAT_PENDING );

        final TapeService tapeService =
                support.getDatabaseSupport().getServiceManager().getService( TapeService.class );

        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "Test Domain" );
        final StorageDomainMember domainMember = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO6 );
        tape.setStorageDomainMemberId( domainMember.getId() );
        tapeService.update( tape, Tape.STORAGE_DOMAIN_MEMBER_ID );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.TAPE + "/" + tape.getId() )
                .addParameter( "operation", RestOperationType.MARK_FOR_COMPACTION.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );

        assertEquals(TapeState.FORMAT_PENDING, tapeService.attain( tape.getId() ).getState(), "State should remain unchanged.");
    }

    // This is a negative test that verifies a 409 is returned if the user attempts to mark a
    // tape for compaction when the the tape's partition has auto compaction disabled.
    @Test
    public void testtestMarkTapeForCompactionWithAutoCompactionDisabledNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );

        final TapePartition tapePartition = mockDaoDriver.createTapePartition( null, null,
                TapeDriveType.LTO6, false );

        final Tape tape = mockDaoDriver.createTape(tapePartition.getId(), TapeState.NORMAL);

        final TapeService tapeService =
                support.getDatabaseSupport().getServiceManager().getService( TapeService.class );

        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "Test Domain" );
        final StorageDomainMember domainMember = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO6 );
        tape.setStorageDomainMemberId( domainMember.getId() );
        tapeService.update( tape, Tape.STORAGE_DOMAIN_MEMBER_ID );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.TAPE + "/" + tape.getId() )
                .addParameter( "operation", RestOperationType.MARK_FOR_COMPACTION.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "State should remain unchanged.");
    }

    // This is a negative test that verifies a 409 is returned if the user attempts to mark a
    // tape for compaction when IOM is disabled.
    @Test
    public void testtestMarkTapeForCompactionWithIomDisabledNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.updateBean(
                mockDaoDriver.attainOneAndOnly(DataPathBackend.class).setIomEnabled(false),
                DataPathBackend.IOM_ENABLED);

        final TapePartition tapePartition = mockDaoDriver.createTapePartition( null, null,
                TapeDriveType.LTO6, true );

        final Tape tape = mockDaoDriver.createTape( tapePartition.getId(), TapeState.NORMAL );

        final TapeService tapeService =
                support.getDatabaseSupport().getServiceManager().getService( TapeService.class );

        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "Test Domain" );
        final StorageDomainMember domainMember = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO6 );
        tape.setStorageDomainMemberId( domainMember.getId() );
        tapeService.update( tape, Tape.STORAGE_DOMAIN_MEMBER_ID );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.TAPE + "/" + tape.getId() )
                .addParameter( "operation", RestOperationType.MARK_FOR_COMPACTION.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "State should remain unchanged.");
    }

    // This is a negative test that verifies a 409 is returned if the user attempts to mark a
    // tape for compaction when it is write protected.
    @Test
    public void testtestMarkTapeForCompactionWriteProtectedNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );

        final TapePartition tapePartition = mockDaoDriver.createTapePartition( null, null,
                TapeDriveType.LTO6, true );

        final Tape tape = mockDaoDriver.createTape( tapePartition.getId(), TapeState.NORMAL );
        tape.setWriteProtected( true );
        support.getDatabaseSupport().getServiceManager().getService( TapeService.class ).
                update( tape, Tape.WRITE_PROTECTED );

        final TapeService tapeService =
                support.getDatabaseSupport().getServiceManager().getService( TapeService.class );

        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "Test Domain" );
        final StorageDomainMember domainMember = mockDaoDriver.addTapePartitionToStorageDomain(
                storageDomain.getId(), tapePartition.getId(), TapeType.LTO6 );
        tape.setStorageDomainMemberId( domainMember.getId() );
        tapeService.update( tape, Tape.STORAGE_DOMAIN_MEMBER_ID );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT,
                "_rest_/" + RestDomainType.TAPE + "/" + tape.getId() )
                .addParameter( "operation", RestOperationType.MARK_FOR_COMPACTION.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );

        assertEquals(TapeState.NORMAL, tapeService.attain( tape.getId() ).getState(), "State should remain unchanged.");
    }
}
