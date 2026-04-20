package com.spectralogic.s3.dataplanner.backend.tape.processor.main;

public enum TapeFailureAction {
    NONE,
    TAPE_MARKED_BAD,
    DRIVE_QUIESCED
}
