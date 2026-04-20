/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.io.lang.ByteRanges;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.math.LongRange;
import com.spectralogic.util.lang.math.LongRangeImpl;

public final class ByteRangesImpl implements ByteRanges
{
    public ByteRangesImpl( final String httpByteRanges, final long totalSize )
    {
        this( parse( httpByteRanges, totalSize ), totalSize );
    }
    
    
    private static List< LongRange > parse( String httpByteRanges, final long totalSize )
    {
        httpByteRanges = httpByteRanges.replace( " ", "" ).toLowerCase();
        if ( !httpByteRanges.startsWith( "bytes=" ) )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.BAD_REQUEST,
                    "Range should have started with prefix 'bytes=': " + httpByteRanges );
        }
        httpByteRanges = httpByteRanges.substring( "bytes=".length() );
        final List< LongRange > retval = new ArrayList<>();
        for ( final String range : httpByteRanges.split( Pattern.quote( "," ) ) )
        {
            long start = 0;
            long end = totalSize - 1;
            if ( !range.contains( "-" ) )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "Range should have contained a dash, but it did not: " + range );
            }
            final String [] parts = range.split( Pattern.quote( "-" ) );
            if ( 0 == parts.length || 2 < parts.length )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "Range invalid: " + range );
            }
            if ( 0 == parts[ 0 ].length() )
            {
                start = totalSize - Long.valueOf( parts[ 1 ] ).longValue();
            }
            else
            {
                start = Long.valueOf( parts[ 0 ] ).longValue();
                if ( 2 == parts.length && 0 != parts[ 1 ].length() )
                {
                    end = Long.valueOf( parts[ 1 ] ).longValue();
                }
            }
            if ( totalSize <= end )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "Range goes beyond end of object: " + range );
            }
            retval.add( new LongRangeImpl( start, end ) );
        }
        
        return retval;
    }
    
    
    private ByteRangesImpl( final List< LongRange > ranges, final long totalSize )
    {
        final List< LongRange > sorted = new ArrayList<>( ranges );
        Collections.sort( sorted, new LongRangeComparator() );
        if ( !ranges.equals( sorted ) )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.BAD_REQUEST,
                    "When multiple ranges are specified, they must be sorted in byte order.  "
                    + ranges + " is an invalid request.  Instead, request " + sorted + "." );
        }
        
        long aggregateLength = 0;
        for ( final LongRange range : ranges )
        {
            aggregateLength += range.getLength();
        }
        m_aggregateLength = aggregateLength;
        
        m_totalSize = totalSize;
        m_ranges = sorted;
        m_fullRange = new LongRangeImpl( 
                m_ranges.get( 0 ).getStart(), 
                m_ranges.get( m_ranges.size() - 1 ).getEnd() );
    }
    
    
    private final static class LongRangeComparator implements Comparator< LongRange >
    {
        public int compare( final LongRange o1, final LongRange o2 )
        {
            if ( o1.overlaps( o2 ) )
            {
                throw new FailureTypeObservableException(
                        GenericFailure.BAD_REQUEST,
                        "When multiple ranges are specified, they must be non-overlapping.  " 
                        + "The following byte ranges overlap: " + CollectionFactory.toList( o1, o2 ) );
            }
            return Long.valueOf( o1.getStart() ).compareTo( Long.valueOf( o2.getStart() ) );
        }
    } // end inner class def
    
    
    public List< LongRange > getRanges()
    {
        return new ArrayList<>( m_ranges );
    }
    
    
    public LongRange getFullRequiredRange()
    {
        return m_fullRange;
    }
    
    
    @Override
    public String toString()
    {
        if ( m_ranges.isEmpty() )
        {
            return "<no ranges>";
        }
        
        String retval = "";
        for ( final LongRange range : m_ranges )
        {
            if ( 0 == retval.length() )
            {
                retval += "bytes ";
            }
            else
            {
                retval += ",";
            }
            retval += range.getStart() + "-" + range.getEnd();
        }
        
        return retval + "/" + m_totalSize;
    }
    
    
    public long getAggregateLength()
    {
        return m_aggregateLength;
    }


    public ByteRanges shift( final long byteOffset )
    {
        if ( byteOffset != 0L )
        {
            final List< LongRange > newRanges = new ArrayList<>();
            for ( final LongRange range : m_ranges )
            {
                newRanges.add( new LongRangeImpl(
                        range.getStart() + byteOffset,
                        range.getEnd() + byteOffset ) );
            }
            return new ByteRangesImpl( newRanges, m_totalSize );
        }
        return this;
    }

    
    private final List< LongRange > m_ranges;
    private final LongRange m_fullRange;
    private final long m_aggregateLength;
    private final long m_totalSize;
}
