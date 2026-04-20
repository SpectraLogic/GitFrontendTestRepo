package com.spectralogic.s3.dataplanner.backend.tape.api;

import com.spectralogic.s3.dataplanner.backend.tape.api.TapeTask;

import java.util.Collection;
import java.util.UUID;

public interface IoTask extends TapeTask {
    Collection<UUID> getChunkIds();
}
