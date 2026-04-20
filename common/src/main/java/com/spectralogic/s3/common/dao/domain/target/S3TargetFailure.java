/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.target;

import java.util.UUID;

import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.CascadeDelete.WhenReferenceIsDeleted;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;
import com.spectralogic.util.db.lang.References;

@Indexes( @Index( S3TargetFailure.DATE ) )
public interface S3TargetFailure extends DatabasePersistable, TargetFailure< S3TargetFailure >
{
    @CascadeDelete( WhenReferenceIsDeleted.DELETE_THIS_BEAN )
    @References( S3Target.class )
    UUID getTargetId();
}
