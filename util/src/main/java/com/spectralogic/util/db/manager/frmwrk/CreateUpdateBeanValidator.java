/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.frmwrk;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.cache.CacheResultProvider;
import com.spectralogic.util.cache.StaticCache;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.MustMatchRegularExpression;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;

public final class CreateUpdateBeanValidator
{
    private CreateUpdateBeanValidator()
    {
        // singleton
    }
    
    
    public static void validate( final Set< String > propertiesToValidate, final DatabasePersistable bean )
    {
        final TypeValidator validator = TYPE_VALIDATOR_CACHE.get( bean.getClass() );
        validator.validate( propertiesToValidate, bean );
    }
    
    
    private final static class TypeValidator
    {
        private TypeValidator( final Class< ? > type )
        {
            for ( final String prop : BeanUtils.getPropertyNames( type ) )
            {
                final Method reader = BeanUtils.getReader( type, prop );
                final MustMatchRegularExpression regex = 
                        reader.getAnnotation( MustMatchRegularExpression.class );
                if ( null == regex )
                {
                    continue;
                }
                
                m_properties.put( prop, reader );
                m_regularExpressions.put( reader, regex.value() );
            }
        }
        
        private void validate( final Set< String > propertiesToValidate, final Object bean )
        {
            Validations.verifyNotNull( "Bean", bean );
            for ( final Map.Entry< String, Method > e : m_properties.entrySet() )
            {
                if ( null != propertiesToValidate && !propertiesToValidate.contains( e.getKey() ) )
                {
                    continue;
                }
                
                final Method reader = e.getValue();
                final String regexp = m_regularExpressions.get( reader );
                try
                {
                    final String value = (String)reader.invoke( bean );
                    if ( null == value )
                    {
                        continue;
                    }
                    
                    if ( !Pattern.matches( regexp, value ) )
                    {
                        throw new DaoException( 
                                GenericFailure.BAD_REQUEST,
                                "Invalid value detected for " + e.getKey() + " (value was '" + value 
                                + "', but value must match regular expression '" 
                                + e.getValue().getAnnotation( MustMatchRegularExpression.class ).value() 
                                + "')." );
                    }
                }
                catch ( final Exception ex )
                {
                    throw ExceptionUtil.toRuntimeException( ex );
                }
            }
        }

        private final Map< String, Method > m_properties = new HashMap<>();
        private final Map< Method, String > m_regularExpressions = new HashMap<>();
    } // end inner class def
    
    
    private final static class TypeValidatorResultProvider 
        implements CacheResultProvider< Class< ? >, TypeValidator >
    {
        public TypeValidator generateCacheResultFor( final Class< ? > param )
        {
            return new TypeValidator( BeanFactory.getType( param ) );
        }
    } // end inner class def
    
    
    private final static StaticCache< Class< ? >, TypeValidator > TYPE_VALIDATOR_CACHE =
            new StaticCache<>( new TypeValidatorResultProvider() );
}
