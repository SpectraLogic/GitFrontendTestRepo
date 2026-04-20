/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.frmwrk;

import java.lang.reflect.Method;
import java.sql.ResultSet;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.lang.DatabaseNamingConvention;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.DatabaseUtils;
import com.spectralogic.util.lang.reflect.ReflectUtil;

/**
 * Unmarshals db response data into Java beans
 */
public final class DatabaseUnmarshaler< T extends DatabasePersistable >
{
    public DatabaseUnmarshaler( final Class< T > clazz )
    {
        m_clazz = BeanFactory.getType( clazz );
    }
    
    
    public T unmarshal( final ResultSet resultSet )
    {
        @SuppressWarnings( "unchecked" )
        final T retval = (T)BeanFactory.newBean( m_clazz );
        
        for ( final String prop : DatabaseUtils.getPersistablePropertyNames( m_clazz ) )
        {
            Object value = null;
            try
            {
                value = resultSet.getObject( DatabaseNamingConvention.toDatabaseColumnName( prop ) );
                final Method writer = BeanUtils.getWriter( m_clazz, prop );
                final Class< ? > type = writer.getParameterTypes()[ 0 ];
                if ( type.isEnum() && null != value )
                {
                    writer.invoke( retval, ReflectUtil.enumValueOf( type, value.toString() ) );
                }
                else
                {
                    writer.invoke( retval, value );
                }
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( 
                        "Failed to set " + value + " on " + m_clazz.getName() + "." + prop, ex );
            }
        }
        
        return retval;
    }
    
    
    private final Class< ? > m_clazz;
}
