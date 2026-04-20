package com.spectralogic.s3.dataplanner.backend.tape.task;

import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDrive;
import com.spectralogic.s3.common.dao.domain.tape.TapeFailureType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.service.tape.TapeDriveService;
import com.spectralogic.s3.common.dao.service.tape.TapeFailureService;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.tape.domain.LoadedTapeInformation;
import com.spectralogic.s3.dataplanner.backend.tape.processor.main.TapeFailureManagement;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import org.apache.log4j.Logger;

import java.util.UUID;

import static com.spectralogic.s3.dataplanner.backend.tape.processor.main.api.internal.TapeEnvironment.CHANGED_BAR_CODE_PREFIX;

class MinimalTapeInDriveVerifier implements BaseTapeTask.TapeInDriveVerifier {
    protected final BaseTapeTask baseTapeTask;

    public MinimalTapeInDriveVerifier(BaseTapeTask baseTapeTask)
    {
        this.baseTapeTask = baseTapeTask;
        m_tapeFailureManagement = baseTapeTask.m_tapeFailureManagement;
    }

    public void run() {
        final String serialNumber = baseTapeTask.getDriveResource().getLoadedTapeSerialNumber().get(RpcFuture.Timeout.LONG);
        final Tape tape = baseTapeTask.getTape();

        try {
            final LoadedTapeInformation tapeInformation =
                    baseTapeTask.getDriveResource().getLoadedTapeInformation().get(RpcFuture.Timeout.LONG);
            baseTapeTask.getServiceManager().getService(TapeService.class).update(
                    tape.setWriteProtected(tapeInformation.isReadOnly()), Tape.WRITE_PROTECTED);
        } catch (final RuntimeException ex) {
            LOG.warn("Failed to get loaded tape information for tape " + baseTapeTask.getTapeId() + ".  "
                    + "Cannot update write protection state.", ex);
        }

        verifyNoSerialNumberMismatch(tape, serialNumber);
    }

    final protected void verifyNoSerialNumberMismatch(
            final Tape tape, final String loadedTapeSerialNumber) {
        final TapeService service = baseTapeTask.getServiceManager().getService(TapeService.class);
        if (null == tape.getSerialNumber()) {
            try {
                service.update(
                        tape.setSerialNumber(loadedTapeSerialNumber),
                        SerialNumberObservable.SERIAL_NUMBER);
                return;
            } catch (final DaoException ex) {
                final Tape correctTape = service.attain(
                        SerialNumberObservable.SERIAL_NUMBER,
                        loadedTapeSerialNumber);

                //NOTE: It should be impossible for this tape record to have data since we don't currently know
                //the serial, but we sanity check before deleting it from the DB. We also check to see if the barcode
                //matches, which would imply two tapes with the same barcode but different partitions might have been
                //swapped, in which case we put them into an error state.
                if (!correctTape.getBarCode().equals(tape.getBarCode())
                        && !tape.isAssignedToStorageDomain()
                        && tape.getStorageDomainMemberId() == null) {
                    //This is not actually a new tape, it's just had its bar code changed, so update the barcode of the
                    //old record and the "new" tape to have a missing bar code. It will be deleted automatically on the
                    //next tape environment reconcile, and the element address for the old record will be updated.
                    final String newBarCode = tape.getBarCode();
                    final String oldBarCode = correctTape.getBarCode();
                    service.update(tape.setBarCode(CHANGED_BAR_CODE_PREFIX + oldBarCode), Tape.BAR_CODE);
                    service.update(correctTape.setBarCode(newBarCode), Tape.BAR_CODE);
                    //NOTE: the existence of a "changed bar code" prefix in the DB will trigger the next possible tape reconcile trigger even if the generation hasn't changed.
                    final RuntimeException ex2 = new RuntimeException( "The bar code appears to have changed from " + correctTape.getBarCode()
                            + " to " + tape.getBarCode(), ex);
                    m_tapeFailureManagement.registerFailure(
                            tape.getId(),
                            TapeFailureType.BAR_CODE_CHANGED, ex2);
                    //Although we have corrected the barcode, we still throw because we are aborting whatever task we
                    //were attempting to do to this "new" tape that isn't actually new.
                    throw ex2;
                } else {
                    service.transistState(tape, TapeState.SERIAL_NUMBER_MISMATCH);
                    service.transistState(correctTape, TapeState.SERIAL_NUMBER_MISMATCH);

                    final RuntimeException ex2 = new RuntimeException(
                            "Two tapes appear to have the same serial number: " + tape.getSerialNumber(),
                            ex);
                    m_tapeFailureManagement.registerFailure(
                            tape.getId(),
                            TapeFailureType.SERIAL_NUMBER_MISMATCH,
                            ex2);
                    throw ex2;
                }
            }
        }

        if (!loadedTapeSerialNumber.equals(tape.getSerialNumber())) {
            baseTapeTask.getServiceManager().getService(TapeService.class).transistState(
                    tape, TapeState.SERIAL_NUMBER_MISMATCH);
            m_tapeFailureManagement.registerFailure(
                    tape.getId(),
                    TapeFailureType.SERIAL_NUMBER_MISMATCH,
                    new RuntimeException("Expected tape to have S/N " + tape.getSerialNumber()
                            + ", but it has S/N " + loadedTapeSerialNumber + "."));
            throw new IllegalStateException(
                    "The tape loaded in the drive should have S/N " + tape.getSerialNumber()
                            + ", but it has S/N " + loadedTapeSerialNumber + ".");
        }
    }


    protected final static Logger LOG = Logger.getLogger( MinimalTapeInDriveVerifier.class );
    protected final TapeFailureManagement m_tapeFailureManagement;
} // end inner class def
