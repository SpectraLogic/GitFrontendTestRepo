/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.render;

import com.spectralogic.util.lang.Duration;

public final class BytesRenderer implements ValueRenderer< Number >
{
    public BytesRenderer()
    {
        this( 10 );
    }
    
    
    public BytesRenderer( final int multipleOfDivisorRequiredToAccept )
    {
        m_multipleOfDivisorRequiredToAccept = multipleOfDivisorRequiredToAccept;
    }
    
    
    public String render( final long rawValue, final Duration duration )
    {
        return render( ( rawValue * 1000 ) / Math.max( 1, duration.getElapsedMillis() ) ) + "/sec";
    }
    
    
    public String render( final long rawValue )
    {
        return render( Long.valueOf( rawValue ) );
    }
    
    
    public String render( final Number rawValue )
    {
        final long bytes = rawValue.longValue();
        
        long divisor = (long)Math.pow( 1024, 3 );
        if ( m_multipleOfDivisorRequiredToAccept * divisor <= bytes )
        {
            return ( bytes / divisor ) + " GiB";
        }
        
        divisor = (long)Math.pow( 1024, 2 );
        if ( m_multipleOfDivisorRequiredToAccept * divisor <= bytes )
        {
            return ( bytes / divisor ) + " MiB";
        }
        
        divisor = (long)Math.pow( 1024, 1 );
        if ( m_multipleOfDivisorRequiredToAccept * divisor <= bytes )
        {
            return ( bytes / divisor ) + " KiB";
        }

        return bytes + " B";
    }
    
    
    private final int m_multipleOfDivisorRequiredToAccept;
}
