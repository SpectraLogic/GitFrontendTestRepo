/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.spectralogic.util.bean.lang.ConcreteImplementation;
import com.spectralogic.util.exception.ExceptionUtil;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.mock.MockObjectFactory;

public final class BeanValidator
{
    public static Set< Violation > test( final Class< ? > clazz )
    {
        Validations.verifyNotNull( "Class", clazz );
        
        final Set< Violation > violations = new HashSet<>();
        
        testAllDeclaredFinalStaticStringsAreUniqueAndDefineAllBeanProps(
                clazz, 
                violations );
        testAllBeanMethodToEnsureThatTheReaderReturnTypeMatchesTheWriterParamType(
                clazz,
                violations );
        testConcreteImplementationCompliance(
                clazz,
                violations );

        for ( final Violation v : violations )
        {
            if ( SeverityType.INFO != v.getSeverity() )
            {
                throw new RuntimeException( "Validation failed: " + violations );
            }
        }
        
        return violations;
    }


    public static class Violation
    {
        private Violation( final SeverityType severity,
                   final String message,
                   final Class<?> testedClass,
                   final String propertyName,
                   final Method method )
        {
            if ( null == severity || null == message || null == testedClass )
            {
                throw new IllegalArgumentException( "Config param cannot be null!" ); 
            }

            m_severity = severity;

            final StringBuilder sb = new StringBuilder( 400 );
            sb.append( m_severity.toString() );
            sb.append( " on " ); 
            sb.append( testedClass.getSimpleName() );
            if ( null != propertyName )
            {
                sb.append( '.' ); 
                sb.append( propertyName );
            }
            if ( null != method )
            {
                sb.append( ", method " ); 
                sb.append( method.getName() );
            }
            sb.append( ":  " ); 
            sb.append( message );

            m_message = sb.toString();
        }

        
        public SeverityType getSeverity()
        {
            return m_severity;
        }

        
        @Override
        public String toString()
        {
            return m_message;
        }

        
        private final SeverityType m_severity;
        private final String m_message;
    } // end inner class

    
    public enum SeverityType
    {
        INFO,
        WARNING,
        ERROR
    } // end enum
    
    
    private static void testAllBeanMethodToEnsureThatTheReaderReturnTypeMatchesTheWriterParamType(
            final Class< ? > beanClass,
            final Set< Violation > violations )
    {
        for ( final String prop : BeanUtils.getPropertyNames( beanClass ) )
        {
            final Method writer = BeanUtils.getWriter( beanClass, prop );
            if ( null == writer )
            {
                continue;
            }
            
            final Method reader = BeanUtils.getReader( beanClass, prop );
            if ( null == reader )
            {
                continue;
            }
            if ( reader.getReturnType() != writer.getParameterTypes()[ 0 ] )
            {
                violations.add( new Violation(
                        SeverityType.ERROR,
                        "The reader return type must match the writer param type.",
                        beanClass,
                        prop,
                        reader ) );
            }
        }
    }

    
    private static void testAllDeclaredFinalStaticStringsAreUniqueAndDefineAllBeanProps( 
            final Class< ? > beanClass,
            final Set< Violation > violations )
    {
        /*
         * Important - please read before considering modifying the 1 line of
         * code below or the method the line of code calls.
         * 
         * Calling beanClass.getFields() occasionally returns a subset of the 
         * fields it is supposed to according to its javadocs IF the class in 
         * question is not imported into this class.  As the classes tested will
         * certainly NOT be imported into this class, this means that calling
         * getFields is not a reliable mechanism for getting all accessible,
         * visible fields.
         * 
         * Furthermore, even if there were a way to get around the above, we
         * want to pick up non-accessible (e.g. private) fields, as there are
         * circumstances where a non-public field for specifying the bean prop
         * is appropriate, making beanClass.getDeclaredFields most appropriate.
         */
        final Set< Field > fields = getStaticStringFields( beanClass );

        final Set< String > beanProps = BeanUtils.getPropertyNames( beanClass );
        beanProps.remove( "class" ); 
        beanProps.remove( "propertyChangeListeners" ); 
        final Map< String, Field > map = new HashMap<>();
        String s;
        for ( Field f : fields )
        {
            try
            {
                f.setAccessible( true );
                s = (String)f.get( null );
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( "Could not read from field.", ex ); 
            }
            
            if ( ! beanProps.contains( s ) )
            {
                continue;
            }
            
            if ( map.containsKey( s ) )
            {
                final String message = new StringBuilder( 200 )
                    .append( "Field " ) 
                    .append( f.getName() )
                    .append( " in " ) 
                    .append( f.getDeclaringClass().getSimpleName() )
                    .append( " has the same value as field " ) 
                    .append( map.get( s ).getName() )
                    .append( " in " ) 
                    .append( beanClass.getSimpleName() )
                    .append( ".  This is likely a copy-paste error." ) 
                    .toString();
                violations.add( new Violation(
                                SeverityType.ERROR,
                                message,
                                beanClass,
                                s,
                                null ) );
            }
            
            final String expectedFieldName = NamingConventionType.CONSTANT.convert( s );
            if ( !f.getName().equals( expectedFieldName ) )
            {
                final String message = new StringBuilder( 200 )
                    .append( "Field " ) 
                    .append( f.getName() )
                    .append( " on " ) 
                    .append( f.getDeclaringClass().getSimpleName() )
                    .append( " does not follow the standard field naming " ) 
                    .append( "convention (this field should be called " ) 
                    .append( expectedFieldName )
                    .append( ")." )
                    .toString();
                violations.add( new Violation(
                                SeverityType.ERROR,
                                message,
                                f.getDeclaringClass(),
                                s,
                                null ) );
            }
            
            map.put( s, f );
        }
        
        for ( String beanProp : beanProps )
        {
            if ( ! map.containsKey( beanProp ) )
            {
                final String message = new StringBuilder( 200 )
                    .append( "Could not locate a static final variable " ) 
                    .append( "declaring bean prop " ) 
                    .append( beanProp )
                    .append( '.' )
                    .toString();
                violations.add( new Violation( 
                                SeverityType.ERROR,
                                message,
                                beanClass,
                                beanProp,
                                null ) );
            }
        }
    }
    
    
    private static Set< Field > getStaticStringFields( final Class<?> clazz )
    {
        if ( null == clazz )
        {
            return new HashSet<>();
        }
        
        final Field [] fields = clazz.getDeclaredFields();
        final Set< Field > retval = new HashSet<>();
        for ( Field f : fields )
        {
            if ( Modifier.isStatic( f.getModifiers() ) 
                            && ! f.isSynthetic() && String.class == f.getType() )
            {
                retval.add( f );
            }
        }
        
        retval.addAll( getStaticStringFields( clazz.getSuperclass() ) );
        for ( Class<?> iClass : clazz.getInterfaces() )
        {
            retval.addAll( getStaticStringFields( iClass ) );
        }
        
        return retval;
    }
    
    
    private static void testConcreteImplementationCompliance(
            final Class< ? > beanClass,
            final Set< Violation > violations )
    {
        if ( null == beanClass.getAnnotation( ConcreteImplementation.class ) )
        {
            return;
        }
        
        final Class< ? > type = beanClass.getAnnotation( ConcreteImplementation.class ).value();
        if ( null == type )
        {
            violations.add( new Violation( 
                    SeverityType.ERROR,
                    ConcreteImplementation.class.getSimpleName() + " did not specify the class.",
                    beanClass,
                    null,
                    null ) );
            return;
        }
        
        if ( !Modifier.isFinal( type.getModifiers() ) )
        {
            violations.add( new Violation( 
                    SeverityType.ERROR,
                    type.getSimpleName() + " must be final.",
                    beanClass,
                    null,
                    null ) );
        }
        
        try
        {
            final Constructor< ? > con = type.getDeclaredConstructor();
            con.newInstance();
            violations.add( new Violation( 
                    SeverityType.ERROR,
                    "The constructor for " + type.getSimpleName() + " cannot be public (" 
                         + BeanFactory.class.getSimpleName() + " should be the only one to instantiate it).",
                    beanClass,
                    null,
                    null ) );
        }
        catch ( final Exception ex )
        {
            Validations.verifyNotNull( "Exception", ex );
        }
        
        try
        {
            final Constructor< ? > con = type.getDeclaredConstructor();
            con.setAccessible( true );
            testConcreteImplementationMethods( con.newInstance() );
        }
        catch ( final Exception ex )
        {
            violations.add( new Violation( 
                    SeverityType.ERROR,
                    "Failed to validate an instance of " + type.getSimpleName() + ": " 
                            + ExceptionUtil.getFullMessage( ex ),
                    beanClass,
                    null,
                    null ) );
        }
    }
    
    
    private static void testConcreteImplementationMethods( final Object bean )
    {
        final Class< ? > beanClass = bean.getClass();
        for ( final String beanProperty : BeanUtils.getPropertyNames( beanClass ) )
        {
            final Method reader = BeanUtils.getReader( beanClass, beanProperty );
            final Method writer = BeanUtils.getWriter( beanClass, beanProperty );
            try
            {
                if ( null == reader )
                {
                    LOG.warn( "No reader for " + beanClass.getSimpleName() + "." + beanProperty + "." );
                    continue;
                }
                
                final Object arg = MockObjectFactory.objectForType( reader.getReturnType() );
                if ( null == writer )
                {
                    LOG.warn( "No writer for " + beanClass.getSimpleName() + "." + beanProperty + "." );
                }
                else
                {
                    writer.invoke( bean, arg );
                }
                final Object result = reader.invoke( bean );
                if ( null != writer )
                {
                    if ( arg != result && !arg.equals( result ) )
                    {
                        throw new RuntimeException( 
                                "Value set (" + arg + ") was not value returned (" + result + ")." );
                    }
                }
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( "Validation failed on bean property " + beanProperty + ".", ex );
            }
        }
    }
    
    
    private final static Logger LOG = Logger.getLogger( BeanValidator.class );
}
