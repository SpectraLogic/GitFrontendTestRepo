/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.xml.sax.Attributes;

import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.marshal.sax.SaxDataHandler;

public final class S3ObjectsToDeleteApiBeanSaxHandler implements SaxDataHandler
{
    public S3ObjectsToDeleteApiBean getDeleteRequest()
    {
        return m_deleteRequest;
    }
    
    
    public void handleStartElement( final String elementName, final Attributes attributes )
    {
        if ( elementName.equalsIgnoreCase( "Delete" ) )
        {
            transitionState( State.ROOT, State.DELETE);
            
            m_deleteRequest = BeanFactory.newBean( S3ObjectsToDeleteApiBean.class );
            m_deleteRequest.setQuiet( false );
            m_objectList = new ArrayList<>();
        }
        else if ( elementName.equalsIgnoreCase( "Quiet" ) )
        {
            transitionState( State.DELETE, State.QUIET );

            m_characterDataStringBuilder = new StringBuilder();
        }
        else if ( elementName.equalsIgnoreCase( "Object" ) )
        {
            transitionState( State.DELETE, State.OBJECT );
            
            m_objectToDelete = BeanFactory.newBean( S3ObjectToDeleteApiBean.class );
        }
        else if ( elementName.equalsIgnoreCase( "Key" ) )
        {
            transitionState( State.OBJECT, State.KEY );
            
            m_characterDataStringBuilder = new StringBuilder();
        }
        else if ( elementName.equalsIgnoreCase( "VersionId" ) )
        {
            transitionState( State.OBJECT, State.VERSION_ID );
    
            m_characterDataStringBuilder = new StringBuilder();
        }
        else
        {
            throwInvalidElement( elementName );
        }
    }
    

    public void handleEndElement( final String elementName )
    {
        if ( elementName.equalsIgnoreCase( "Delete" ) )
        {
            transitionState( State.DELETE, State.ROOT );
            
            m_deleteRequest.setObjectsToDelete( m_objectList );
            m_objectList = null;
        }
        else if ( elementName.equalsIgnoreCase( "Quiet" ) )
        {
            transitionState( State.QUIET, State.DELETE );

            m_deleteRequest.setQuiet( m_characterDataStringBuilder.toString().equalsIgnoreCase( "true" ) );
            m_characterDataStringBuilder = null;
        }
        else if ( elementName.equalsIgnoreCase( "Object" ) )
        {
            transitionState( State.OBJECT, State.DELETE );

            m_objectList.add( m_objectToDelete );
            m_objectToDelete = null;
        }
        else if ( elementName.equalsIgnoreCase( "Key" ) )
        {
            transitionState( State.KEY, State.OBJECT );
            
            m_objectToDelete.setKey( m_characterDataStringBuilder.toString() );
            m_characterDataStringBuilder = null;
        }
        else if ( elementName.equalsIgnoreCase( "VersionId" ) )
        {
            transitionState( State.VERSION_ID, State.OBJECT );
    
            m_objectToDelete.setVersionId( UUID.fromString( m_characterDataStringBuilder.toString() ) );
            m_characterDataStringBuilder = new StringBuilder();
        }
        else
        {
            throwInvalidElement( elementName );
        }
    }
    
    
    public void handleBodyData( final char[] data, final int start, final int length )
    {
        if (m_characterDataStringBuilder != null)
        {
            m_characterDataStringBuilder.append( data, start, length );
        }
    }
    
    
    private void transitionState( final State expectedState, final State newState )
    {
        if ( !expectedState.equals( m_currentState ) )
        {
            throw new S3RestException(
                    GenericFailure.BAD_REQUEST,
                    String.format(
                            "Invalid element structure. Expected this element to be under "
                            + "the %s node, but was under %s.",
                            expectedState.toString(),
                            m_currentState ) );
        }
        m_currentState = newState;
    }


    private static void throwInvalidElement( final String elementName )
    {
        throw new S3RestException(
                GenericFailure.BAD_REQUEST,
                String.format( "Invalid element '%s'.", elementName ) );
    }
    
    
    private enum State
    {
        ROOT,
        DELETE,
        QUIET,
        OBJECT,
        KEY,
        VERSION_ID;
        
        @Override
        public String toString()
        {
            if (this == ROOT)
            {
                return "document root";
            }
            final String name = this.name();
            return name.substring( 0, 1 ) + name.substring( 1 ).toLowerCase();
        }
    }// end inner class
    
    
    private State m_currentState = State.ROOT;
    

    private StringBuilder m_characterDataStringBuilder;
    private S3ObjectToDeleteApiBean m_objectToDelete;
    private List< S3ObjectToDeleteApiBean > m_objectList;
    private S3ObjectsToDeleteApiBean m_deleteRequest;
}
