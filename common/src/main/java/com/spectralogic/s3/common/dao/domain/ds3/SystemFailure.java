/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.ds3;

import com.spectralogic.s3.common.dao.domain.shared.Failure;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Index;
import com.spectralogic.util.db.lang.Indexes;

@Indexes( @Index( SystemFailure.DATE ) )
public interface SystemFailure extends DatabasePersistable, Failure< SystemFailure, SystemFailureType >
{
    SystemFailureType getType();

    SystemFailure setType( final SystemFailureType value );
}
