/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.request;

import com.spectralogic.util.http.RequestType;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class RequestType_Test
{
    @Test
    public void testAllEnumConstantsAreUppercase()
    {
        for ( final RequestType v : RequestType.class.getEnumConstants() )
        {
            assertEquals(
                    v.toString().toUpperCase(),
                    v.toString(),
                    "All enums must be in uppercase."
                     );
        }
    }
    
    @Test
    public void testAllEnumConstantsAreSorted()
    {
        RequestType lastValue = null;
        for ( final RequestType v : RequestType.class.getEnumConstants() )
        {
            if ( null != lastValue )
            {
                assertTrue(
                        0 < v.toString().compareTo( lastValue.toString() ),
                        "Enum constants shoulda been sorted alphabetically: " 
                                + v + " and " + lastValue + " are not sorted correctly"
                         );
            }
            lastValue = v;
        }
    }
}
