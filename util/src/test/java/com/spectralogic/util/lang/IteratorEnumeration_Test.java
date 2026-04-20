/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang;

import java.util.Enumeration;
import java.util.List;




import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class IteratorEnumeration_Test 
{

    @Test
    public void testTransformationFromIteratorToEnumerationIsCorrect()
    {
        final List< String > list = CollectionFactory.toList( "a", "b", "c" );
        final Enumeration< String > enumeration =
                new IteratorEnumeration<>( list.iterator() );
        assertTrue(enumeration.hasMoreElements(), "Shoulda reported there's an 'a' next.");
        assertEquals("a", enumeration.nextElement(), "Shoulda");
        assertTrue(enumeration.hasMoreElements(), "Shoulda reported there's an 'b' next.");
        assertEquals("b", enumeration.nextElement(), "Shoulda");
        assertTrue(enumeration.hasMoreElements(), "Shoulda reported there's an 'c' next.");
        assertEquals("c", enumeration.nextElement(), "Shoulda");
        assertFalse(
                enumeration.hasMoreElements(),
                "Shoulda reported nothing's next.");
    }
}
