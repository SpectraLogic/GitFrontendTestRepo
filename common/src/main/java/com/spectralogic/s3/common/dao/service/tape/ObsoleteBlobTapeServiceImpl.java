/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.ObsoleteBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.service.obsoleteblob.BaseObsoleteBlobService;

final class ObsoleteBlobTapeServiceImpl
        extends BaseObsoleteBlobService< ObsoleteBlobTape, Tape > implements ObsoleteBlobTapeService
{
    ObsoleteBlobTapeServiceImpl()
    {
        super( ObsoleteBlobTape.class,
                Tape.class,
                BlobTape.TAPE_ID,
                PersistenceTarget.LAST_VERIFIED, Tape.PARTIALLY_VERIFIED_END_OF_TAPE );
    }
}
