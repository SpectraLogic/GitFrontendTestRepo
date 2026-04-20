/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.target.frmwrk;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public final class ContentsSegment
{
    public ContentsSegment( final Set< String > keys, final String nextMarker )
    {
        m_nextMarker = nextMarker;
        parseSegment( keys );
    }
    
    
    private void parseSegment( final Set< String > keys )
    {
        for ( final String key : keys )
        {
            if ( key.contains( BasePublicCloudConnection.BLOB_PREFIX )
                    && key.endsWith( BasePublicCloudConnection.FIRST_BLOB_SUFFIX ) )
            {
                String blobId = 
                        key.substring( key.indexOf( BasePublicCloudConnection.BLOB_PREFIX )
                                       + BasePublicCloudConnection.BLOB_PREFIX.length() );
                blobId = blobId.substring( 0, blobId.indexOf( '.' ) );
                
                String objectId = 
                        key.substring( BasePublicCloudConnection.DATA.length() 
                                       + BasePublicCloudConnection.SEPARATOR.length() );
                objectId = objectId.substring( 0, objectId.indexOf( BasePublicCloudConnection.SEPARATOR ) );
                
                try
                {
                    final UUID bid = UUID.fromString( blobId );
                    final UUID oid = UUID.fromString( objectId );
                    if ( !m_segment.containsKey( oid ) )
                    {
                        m_segment.put( oid, new HashSet< UUID >() );
                    }
                    m_segment.get( oid ).add( bid );
                    ++m_blobCount;
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException(
                            "Failed to parse object id '" + objectId + "' and blob id '" + blobId + "'.", 
                            ex );
                }
            }
        }
    }
    
    
    public int getBlobCount()
    {
        return m_blobCount;
    }
    
    
    public String getNextMarker()
    {
        return m_nextMarker;
    }
    
    
    public Map< UUID, Set< UUID > > getSegment()
    {
        return m_segment;
    }
    
    
    private int m_blobCount;
    private final String m_nextMarker;
    private final Map< UUID, Set< UUID > > m_segment = new HashMap<>();
}