/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean.lang;

import java.util.UUID;

public interface Identifiable extends SimpleBeanSafeToProxy
{
    String ID = "id";
    
    UUID getId();
    
    Identifiable setId( final UUID value );
}
