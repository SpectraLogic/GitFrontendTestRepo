/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.notification.generator;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.XmlMarshaler;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.notification.domain.NotificationPayload;

final class NotificationPayloadTracker
{
    synchronized static void register( final NotificationPayload payload )
    {
        Validations.verifyNotNull( "Payload", payload );
        final Class< ? extends NotificationPayload > clazz = 
                InterfaceProxyFactory.getType( payload.getClass() );
        
        PayloadType type = null;
        final String name = clazz.getSimpleName();
        List< PayloadType > pts = PAYLOADS.getTypes();
        if ( null == pts )
        {
            pts = new ArrayList<>();
        }
        for ( final PayloadType pt : pts )
        {
            if ( pt.getName().equals( name ) )
            {
                type = pt;
            }
        }
        if ( null == type )
        {
            type = BeanFactory.newBean( PayloadType.class ).setName( name );
            List< PayloadType > types = PAYLOADS.getTypes();
            if ( null == types )
            {
                types = new ArrayList<>();
            }
            types.add( type );
            PAYLOADS.setTypes( types );
        }
        
        List< String > payloads = type.getPayload();
        if ( null == payloads )
        {
            payloads = new ArrayList<>();
        }
        payloads.add( payload.toXml() );
        type.setPayload( payloads );
        
        /*
         * The line of code below is to be commented out at ALL times; except when re-generating documentation
         * be sure when you uncomment out the line below that you do NOT accidentally check the change into
         * Perforce.
         */
//        persistPayloads();
    }
    
    
    synchronized static AllPayloads getPayloads()
    {
        return PAYLOADS;
    }
    
    
    interface AllPayloads extends SimpleBeanSafeToProxy
    {
        String TYPES = "types";
        
        @CustomMarshaledName( 
                value = "Type",
                collectionValue = "NotificationPayloads",
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        List< PayloadType > getTypes();
        
        void setTypes( final List< PayloadType > values );
    } // end inner class def
    
    
    interface PayloadType extends SimpleBeanSafeToProxy
    {
        String NAME = "name";
        
        String getName();
        
        PayloadType setName( final String value );
        
        
        String PAYLOAD = "payload";
        
        List< String > getPayload();
        
        void setPayload( final List< String > value );
    } // end inner class def
    
    
    static void persistPayloads()
    {
        try
        {
            final File file = new File(
                    System.getProperty( "java.io.tmpdir" ) + Platform.FILE_SEPARATOR + FILE_NAME );
            final FileWriter out = new FileWriter( file );
            out.write( XmlMarshaler.formatPretty( PAYLOADS.toXml() ) );
            out.close();
            
            LOG.info( "Wrote notification payloads to " + file + "." );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }
    
    
    private final static AllPayloads PAYLOADS = BeanFactory.newBean( AllPayloads.class );
    final static String FILE_NAME = "notification-payloads.xml";
    private final static Logger LOG = Logger.getLogger( NotificationPayloadTracker.class );
}
