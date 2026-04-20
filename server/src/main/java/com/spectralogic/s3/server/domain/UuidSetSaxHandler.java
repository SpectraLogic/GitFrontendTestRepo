/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;

import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.marshal.sax.SaxDataHandler;
import com.spectralogic.util.marshal.sax.SaxParser;

public final class UuidSetSaxHandler implements SaxDataHandler
{
    /**
     * @return null if the ids could not be extracted from the request payload; non-null otherwise
     */
    public static Set< UUID > extractIds( final DS3Request request )
    {
        try
        {
            final UuidSetSaxHandler handler = new UuidSetSaxHandler();
            final SaxParser sparser = new SaxParser();
            sparser.addHandler( handler );
            sparser.setInputStream( request.getHttpRequest().getInputStream() );
            sparser.parse();
            return handler.getIds();
        }
        catch ( final Exception ex )
        {
            LOG.info( "Failed to extract IDs from request payload.", ex );
            return null;
        }
    }
    
    
    public Set< UUID > getIds()
    {
        return m_retval;
    }
    
    
    public void handleStartElement( final String elementName, final Attributes attributes )
    {
        m_idInProgress = new StringBuilder();
    }
    

    public void handleEndElement( final String elementName )
    {
        if ( null != m_idInProgress )
        {
            final String id = m_idInProgress.toString();
            m_idInProgress = null;
            try
            {
                m_retval.add( UUID.fromString( id ) );
            }
            catch ( final RuntimeException ex )
            {
                throw new S3RestException( GenericFailure.BAD_REQUEST, "Not a valid UUID: " + id, ex );
            }
        }
    }
    
    
    public void handleBodyData( final char[] data, final int start, final int length )
    {
        m_idInProgress.append( data, start, length );
    }
    
    
    private StringBuilder m_idInProgress;
    private final Set< UUID > m_retval = new HashSet<>();
    
    private final static Logger LOG = Logger.getLogger( UuidSetSaxHandler.class );
}
