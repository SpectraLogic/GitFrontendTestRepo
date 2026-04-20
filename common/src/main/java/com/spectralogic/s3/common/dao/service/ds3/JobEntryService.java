/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.io.lang.ByteRanges;

public interface JobEntryService
    extends BeansRetriever<JobEntry>, BeanCreator<JobEntry>, BeanUpdater<JobEntry>, BeanDeleter
{
    int getNextChunkNumber( final UUID jobId );
    
    
    long getSizeInBytes( final UUID chunkId );


    JobEntry getCounterpartPutChunk(final UUID chunkId );

    void verifyEntriesExist(final Collection<UUID> ids);

    JobEntry getEntryForS3Request(
            final JobRequestType requestType,
            final UUID objectId,
            final UUID jobId,
            final boolean includeNaked,
            final UUID blobId );

    List<LocalBlobDestination> getTapeDestinations(final UUID chunkId );

    List<LocalBlobDestination> getPoolDestinations(final UUID chunkId );

    List<Ds3BlobDestination> getDs3Destinations(final UUID chunkId );

    List<AzureBlobDestination> getAzureDestinations(final UUID chunkId );

    List<S3BlobDestination> getS3Destinations(final UUID chunkId );
}
