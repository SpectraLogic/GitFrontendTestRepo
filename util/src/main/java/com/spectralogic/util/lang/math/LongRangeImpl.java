/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.math;

import com.spectralogic.util.lang.Validations;

public final class LongRangeImpl implements LongRange
{
    public LongRangeImpl( final long start, final long end )
    {
        m_start = start;
        m_end = end;
        if ( end < start )
        {
            throw new IllegalArgumentException( "End cannot come before start." );
        }
    }


    public boolean overlaps( final LongRange other )
    {
        Validations.verifyNotNull( "Other", other );
        
        final LongRange r1;
        final LongRange r2;
        if ( other.getStart() < getStart() )
        {
            r1 = other;
            r2 = this;
        }
        else
        {
            r1 = this;
            r2 = other;
        }
        
        return ( r2.getStart() <= r1.getEnd() );
    }
    
    
    @Override
    public boolean equals( final Object other )
    {
        if ( this == other )
        {
            return true;
        }
        if ( null == other )
        {
            return false;
        }
        if ( !( other instanceof LongRangeImpl ) )
        {
            return false;
        }
        
        final LongRangeImpl castedOther = (LongRangeImpl)other;
        return ( castedOther.getStart() == getStart() && castedOther.getEnd() == getEnd() );
    }


    @Override
    public int hashCode()
    {
        return Long.valueOf( m_start + m_end ).hashCode();
    }


    public long getStart()
    {
        return m_start;
    }


    public long getEnd()
    {
        return m_end;
    }
    
    
    public long getLength()
    {
        return m_end - m_start + 1;
    }
    
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + getStart() + "-" + getEnd() + "]";
    }


    private final long m_start;
    private final long m_end;
}
