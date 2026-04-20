package com.spectralogic.util.exception;

import java.util.UUID;

public class BlobReadFailedException extends RuntimeException {
    private final UUID blobIds;

    public BlobReadFailedException(final UUID blobId, final String message )
    {
        super( message );
        this.blobIds = blobId;
    }

    public UUID getBlobIds()
    {
        return blobIds;
    }
}