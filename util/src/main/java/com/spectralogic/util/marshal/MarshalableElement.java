/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.marshal;

import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;

public final class MarshalableElement implements Comparable< MarshalableElement >
{
    MarshalableElement(
            final String name, 
            final String collectionName,
            final CollectionNameRenderingMode collectionNameRenderingMode, 
            final ElementRenderingMode elementRenderingMode,
            Object value )
    {
        m_name = name;
        m_collectionName = collectionName;
        m_collectionNameRenderingMode = collectionNameRenderingMode;
        m_renderingMode = elementRenderingMode;
        m_value = value;
        
        Validations.verifyNotNull( "Name", m_name );
        Validations.verifyNotNull( "Rendering mode", m_renderingMode );
        if ( ( null == m_collectionName || m_collectionName.isEmpty() ) !=
             ( CollectionNameRenderingMode.UNDEFINED == m_collectionNameRenderingMode ) )
        {
            throw new RuntimeException( 
                    "You must specify both the collection name and collection name rendering mode " 
                    + "or neither for " + toString() + "." );
        }
    }
    
    
    public String getName()
    {
        return m_name;
    }
    
    
    public String getCollectionName()
    {
        return m_collectionName;
    }
    
    
    public CollectionNameRenderingMode getCollectionNameRenderingMode()
    {
        return m_collectionNameRenderingMode;
    }
    
    
    public Object getValue()
    {
        return m_value;
    }
    
    
    public enum ElementRenderingMode
    {
        ATTRIBUTE,
        CHILD
    }
    
    
    public ElementRenderingMode getElementRenderingMode()
    {
        return m_renderingMode;
    }
    
    
    public int compareTo( final MarshalableElement arg0 )
    {
        final String src = ( null == m_collectionName ) ? m_name : m_collectionName;
        final String dest = ( null == arg0.getCollectionName() ) ? arg0.getName() : arg0.getCollectionName();
        return src.compareTo( dest );
    }
    
    
    @Override
    public String toString()
    {
        return getClass().getName() + "@" + hashCode() + "[name=" + m_name + ", collectionName=" 
                + m_collectionName + ", collectionNameRenderingMode=" + m_collectionNameRenderingMode 
                + ", value=" + m_value + "]";
    }
    
    
    private final ElementRenderingMode m_renderingMode;
    private final String m_name;
    private final String m_collectionName;
    private final CollectionNameRenderingMode m_collectionNameRenderingMode;
    private final Object m_value;
}
