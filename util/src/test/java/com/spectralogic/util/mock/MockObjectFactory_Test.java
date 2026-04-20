/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.mock;

import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.lang.CollectionFactory;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class MockObjectFactory_Test 
{
    @Test
    public void testObjectsForTypesNullTypesNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                MockObjectFactory.objectsForTypes( null );
            }
        } );
    }
    
    
    @Test
    public void testObjectsForTypesReturnsMockedObjects()
    {
        final List< Object > objects = MockObjectFactory.objectsForTypes(
                CollectionFactory.< Class< ? > >toList( int.class, Long.class, String.class ) );
        assertEquals(Integer.valueOf( 0 ), objects.get( 0 ), "Shoulda mocked types.");
        assertEquals(Long.valueOf( 0 ), objects.get( 1 ), "Shoulda mocked types.");
        assertEquals("", objects.get( 2 ), "Shoulda mocked types.");
        assertEquals(3,  objects.size(), "Shoulda mocked types.");
    }
    
    
    @Test
    public void testObjectForTypeNullTypeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                MockObjectFactory.objectForType( null );
            }
        } );
    }
    
    
    @Test
    public void testObjectForTypeReturnsMockedObject()
    {
        assertEquals(Integer.valueOf( 0 ),
                MockObjectFactory.objectForType( int.class ),
                "Shoulda mocked types.");
        assertEquals(Integer.valueOf( 0 ),
                MockObjectFactory.objectForType( Integer.class ),
                "Shoulda mocked types.");
        assertEquals(Long.valueOf( 0 ),
                MockObjectFactory.objectForType( long.class) ,
                        "Shoulda mocked types.");
        assertEquals(Boolean.FALSE, MockObjectFactory.objectForType( boolean.class ), "Shoulda mocked types.");
        assertNotNull(
                MockObjectFactory.objectForType( List.class ),
                "Shoulda mocked types."
                 );
    }
}
