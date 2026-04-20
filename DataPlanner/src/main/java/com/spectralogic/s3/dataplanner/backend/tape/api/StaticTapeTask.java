package com.spectralogic.s3.dataplanner.backend.tape.api;

import java.util.UUID;

public interface StaticTapeTask extends TapeTask {
    //NOTE: consider adding some assurance that static tape tasks cannot have null as tape id at construction time
    UUID getTapeId();

    boolean allowMultiplePerTape();

    default boolean canUseTapeAlreadyInDrive(final TapeAvailability tapeAvailability ) {
        if (getTapeId() == null) {
            return false;
        }
        return getTapeId().equals(tapeAvailability.getTapeInDrive());
    }

    default boolean canUseAvailableTape(final TapeAvailability tapeAvailability) {
        return tapeAvailability.getAvailableTapes().contains(getTapeId());
    }
}
