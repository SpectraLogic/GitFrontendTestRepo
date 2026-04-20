/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;
import java.util.TreeSet;

/**
 * A {@link Properties} that always provides keys sorted, and always stores properties sorted when {@link
 * Properties#store} is called.
 */
public final class SortedProperties extends Properties
{
    @Override
    public synchronized Enumeration< Object > keys()
    {
        return Collections.enumeration( new TreeSet<>( super.keySet() ) );
    }
}
