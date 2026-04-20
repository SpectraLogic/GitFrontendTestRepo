/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.temp;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.shared.BlobObservable;
import com.spectralogic.s3.common.dao.domain.target.BlobAzureTarget;
import com.spectralogic.s3.common.dao.domain.target.BlobTarget;
import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique({ BlobObservable.BLOB_ID, BlobTarget.TARGET_ID }),
})
public interface BlobAzureTargetToVerify extends BlobAzureTarget
{
    @CascadeDelete
    @References( BlobAzureTarget.class )
    UUID getId();
}
