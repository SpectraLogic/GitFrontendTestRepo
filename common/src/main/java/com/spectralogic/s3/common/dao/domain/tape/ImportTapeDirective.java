/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.domain.tape;

import com.spectralogic.s3.common.dao.domain.shared.ImportTapeTargetDirective;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

/**
 * Any tape in state {@link TapeState#IMPORT_PENDING} or {@link TapeState#IMPORT_IN_PROGRESS} will have one
 * of these records to keep track of the parameters of the import directive.  This record will be used to
 * restart the import with the same parameters as was originally requested in the import.
 */
@UniqueIndexes( @Unique( ImportTapeTargetDirective.TAPE_ID ) )
public interface ImportTapeDirective 
    extends DatabasePersistable, ImportTapeTargetDirective< ImportTapeDirective >
{
    // empty
}
