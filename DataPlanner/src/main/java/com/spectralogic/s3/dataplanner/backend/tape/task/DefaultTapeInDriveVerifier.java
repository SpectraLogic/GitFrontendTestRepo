package com.spectralogic.s3.dataplanner.backend.tape.task;

import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.dao.orm.TapeRM;
import com.spectralogic.s3.common.dao.service.tape.TapeService;
import com.spectralogic.s3.common.rpc.tape.domain.LoadedTapeInformation;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import org.apache.log4j.Logger;

final class DefaultTapeInDriveVerifier extends MinimalTapeInDriveVerifier {

    protected DefaultTapeInDriveVerifier(BaseTapeTask baseTapeTask,
                                         final boolean allowThisTaskToProceedWhenTapeIsForeign,
                                         final boolean verifyQuiescedToCheckpoint) {
        super(baseTapeTask);
        m_allowThisTaskToProceedWhenTapeIsForeign = allowThisTaskToProceedWhenTapeIsForeign;
        m_verifyQuiescedToCheckpoint = verifyQuiescedToCheckpoint;
    }


    protected DefaultTapeInDriveVerifier(BaseTapeTask baseTapeTask,
                                         final boolean allowThisTaskToProceedWhenTapeIsForeign) {
        this(baseTapeTask, allowThisTaskToProceedWhenTapeIsForeign, true);
    }

    @Override
    public void run() {
        final LoadedTapeInformation tapeInformation =
                baseTapeTask.getDriveResource().getLoadedTapeInformation().get(RpcFuture.Timeout.LONG);
        final Tape tape = baseTapeTask.getTape();
        baseTapeTask.getServiceManager().getService(TapeService.class).update(
                tape.setWriteProtected(tapeInformation.isReadOnly()), Tape.WRITE_PROTECTED);

        verifyNoSerialNumberMismatch(tape, tapeInformation.getSerialNumber());
        verifyTapeNotForeign(tape, tapeInformation);
        verifyNoTapeTypeMismatch(tape, tapeInformation);
        if (m_verifyQuiescedToCheckpoint) {
            verifyQuiescedToCorrectCheckpoint(tape);
        }
    }

    private void verifyNoTapeTypeMismatch(final Tape tape, final LoadedTapeInformation tapeInformation) {
        if (tape.getType() == tapeInformation.getType()) {
            return;
        }

        if (TapeType.UNKNOWN != tape.getType()) {
            LOG.warn("Tape type changed for " + tape.getId()
                    + " (was " + tape.getType() + ", but is " + tapeInformation.getType() + ").");
        }
        baseTapeTask.getServiceManager().getService(TapeService.class).update(
                tape.setType(tapeInformation.getType()),
                Tape.TYPE);
    }

    private void verifyTapeNotForeign(final Tape tape, final LoadedTapeInformation tapeInformation) {
        if (null == tapeInformation.getTapeId() || tapeInformation.getTapeId().equals(baseTapeTask.getTapeId())) {
            return;
        }
        if (tape.isTakeOwnershipPending()) {
            LOG.info("Tape " + tape.getId()
                    + " has take ownership pending, so will allow task execution to proceed.");
            return;
        }
        if (null != tape.getLastCheckpoint()) {
            if (!baseTapeTask.getDriveResource().hasChangedSinceCheckpoint(tape.getLastCheckpoint())
                    .get(RpcFuture.Timeout.VERY_LONG)) {
                LOG.warn("Tape " + tape.getId() + " (" + tape.getBarCode() + ") had foreign ownership, "
                        + "but its data hasn't changed since its last known checkpoint.  "
                        + "Will automatically take ownership of it back.");
                final boolean ltfsForeign =
                        tape.getState() == TapeState.LTFS_WITH_FOREIGN_DATA ||
                                tape.getState() == TapeState.RAW_IMPORT_IN_PROGRESS ||
                                tape.getState() == TapeState.RAW_IMPORT_PENDING;
                final TapeDriveType driveType =
                        new TapeRM(baseTapeTask.getTape(), baseTapeTask.getServiceManager()).getPartition().getDriveType();
                if (ltfsForeign) {
                    LOG.warn("Cannot take ownership since tape " + tape.getId() + " ("
                            + tape.getBarCode() + ") is LTFS with foreign data.");
                } else if (tapeInformation.isReadOnly()) {
                    LOG.warn("Cannot take ownership at this time since tape " + tape.getId() + " ("
                            + tape.getBarCode() + ") is write-protected.");
                    baseTapeTask.getServiceManager().getService(TapeService.class).update(
                            tape.setTakeOwnershipPending(true),
                            Tape.TAKE_OWNERSHIP_PENDING);
                } else if (!driveType.isWriteSupported(tape.getType())) {
                    LOG.warn("Cannot take ownership since tape " + tape.getId() + " (" + tape.getBarCode()
                            + ") is of type " + tape.getType() + " and is not writeable in"
                            + " a partition with drives of type " + driveType + ".");
                } else {
                    final String checkpoint =
                            baseTapeTask.getDriveResource().takeOwnershipOfTape(tape.getId()).get(
                                    RpcFuture.Timeout.VERY_LONG);
                    baseTapeTask.getServiceManager().getService(TapeService.class).update(
                            tape.setLastCheckpoint(checkpoint),
                            Tape.LAST_CHECKPOINT);
                }
                return;
            }
        }

        if (m_allowThisTaskToProceedWhenTapeIsForeign) {
            LOG.warn("Will proceed, even though the tape (" + tape.getBarCode() + ") should have ID "
                    + tape.getId() + ", but has ID " + tapeInformation.getTapeId()
                    + ", suggesting that it is a foreign tape formatted by another appliance.");
            return;
        }

        baseTapeTask.getServiceManager().getService(TapeService.class).transistState(tape, TapeState.FOREIGN);
        throw new IllegalStateException(
                "The tape loaded in the drive should have ID " + tape.getId()
                        + ", but it has ID " + tapeInformation.getTapeId()
                        + ", suggesting that it is a foreign tape formatted by another appliance.");
    }

    private void verifyQuiescedToCorrectCheckpoint(final Tape tape) {
        TapeTaskUtils.verifyQuiescedToCheckpoint(
                tape,
                baseTapeTask.getDriveResource(),
                baseTapeTask.getServiceManager(),
                m_tapeFailureManagement,
                TapeTaskUtils.RestoreExpected.NO,
                TapeTaskUtils.FailureHandling.THROW_EXCEPTION);
    }

    private final boolean m_allowThisTaskToProceedWhenTapeIsForeign;
    private final boolean m_verifyQuiescedToCheckpoint;
    protected final static Logger LOG = Logger.getLogger( DefaultTapeInDriveVerifier.class );
} // end inner class def
