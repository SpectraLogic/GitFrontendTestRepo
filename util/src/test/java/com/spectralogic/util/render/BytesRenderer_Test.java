/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.render;



import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.lang.Duration;

public final class BytesRenderer_Test 
{
    @Test
    public void testRenderReturnsCorrectlyRenderedValue()
    {
        final BytesRenderer renderer = new BytesRenderer( 10 );

        assertEquals("512 B", renderer.render( Integer.valueOf( 512 ) ), "Shoulda rendered correctly.");
        assertEquals("1024 B", renderer.render( Integer.valueOf( 1024 ) ), "Shoulda rendered correctly.");
        assertEquals("10000 B", renderer.render( Integer.valueOf( 10000 ) ), "Shoulda rendered correctly.");
        assertEquals("10 KiB", renderer.render( Integer.valueOf( 10 * 1024 ) ), "Shoulda rendered correctly.");
        assertEquals("100 KiB", renderer.render( Integer.valueOf( 100 * 1024 ) ), "Shoulda rendered correctly.");
        assertEquals("1000 KiB", renderer.render( Integer.valueOf( 1000 * 1024 ) ), "Shoulda rendered correctly.");
        assertEquals("10000 KiB", renderer.render( Integer.valueOf( 10000 * 1024 ) ), "Shoulda rendered correctly.");
        assertEquals("10 MiB", renderer.render( Integer.valueOf( 10 * 1024 * 1024 ) ), "Shoulda rendered correctly.");
        assertEquals("100 MiB", renderer.render( Integer.valueOf( 100 * 1024 * 1024 ) ), "Shoulda rendered correctly.");
        assertEquals("1000 MiB", renderer.render( Integer.valueOf( 1000 * 1024 * 1024 ) ), "Shoulda rendered correctly.");
        assertEquals("10000 MiB", renderer.render( Long.valueOf( (long)10000 * 1024 * 1024 ) ), "Shoulda rendered correctly.");
        assertEquals("10 GiB", renderer.render( Long.valueOf( (long)10 * 1024 * 1024 * 1024 ) ), "Shoulda rendered correctly.");
        assertEquals("100 GiB", renderer.render( Long.valueOf( (long)100 * 1024 * 1024 * 1024 ) ), "Shoulda rendered correctly.");
        assertEquals("1000 GiB", renderer.render( Long.valueOf( (long)1000 * 1024 * 1024 * 1024 ) ), "Shoulda rendered correctly.");
        assertEquals("10000 GiB", renderer.render( Long.valueOf( (long)10000 * 1024 * 1024 * 1024 ) ), "Shoulda rendered correctly.");

        final Duration duration = new Duration( System.nanoTime() - 1000L * 1000 );
        assertTrue(renderer.render( 5000000, duration ).contains( "MiB/sec" ), "Shoulda rendered correctly.");
        assertTrue(renderer.render( 5000, duration ).contains( "KiB/sec" ), "Shoulda rendered correctly.");
        assertTrue(renderer.render( 1, new Duration() ).contains( "B/sec" ), "Shoulda rendered correctly.");
        assertTrue(renderer.render( 0, new Duration() ).contains( "B/sec" ), "Shoulda rendered correctly.");
    }
}
