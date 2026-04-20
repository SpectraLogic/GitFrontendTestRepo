/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.lang;

import java.util.UUID;

import com.spectralogic.util.bean.lang.Identifiable;

/**
 * A bean that meets the SimpleBeanSafeToProxy requirements that is persistable to database.
 */
public interface DatabasePersistable extends Identifiable
{
    DatabasePersistable setId( final UUID id );
}
