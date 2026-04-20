/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.tape;

import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.service.suspectblob.BaseSuspectBlobService;

final class SuspectBlobTapeServiceImpl 
    extends BaseSuspectBlobService< SuspectBlobTape, Tape > implements SuspectBlobTapeService
{
    SuspectBlobTapeServiceImpl()
    {
        super( SuspectBlobTape.class, 
               Tape.class, 
               BlobTape.TAPE_ID,
               PersistenceTarget.LAST_VERIFIED, Tape.PARTIALLY_VERIFIED_END_OF_TAPE );
    }
}
