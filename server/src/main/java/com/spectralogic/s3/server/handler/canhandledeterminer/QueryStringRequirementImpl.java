/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.canhandledeterminer;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.frmwrk.UserInputValidations;
import com.spectralogic.s3.server.handler.canhandledeterminer.RequestHandlerRequestContract.RequestHandlerParamContract;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.ExcludeFromDocumentation;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.marshal.MarshalUtil;
import com.spectralogic.util.mock.MockObjectFactory;

public final class QueryStringRequirementImpl implements QueryStringRequirement
{
    public boolean meetsRequirement( final DS3Request request )
    {
        return ( getFailures( request ).isEmpty() );
    }
    
    
    public Set< String > getFailures( final DS3Request request )
    {
        final Set< String > fromRequest = new HashSet<>(
                request.getBeanPropertyValueMapFromRequestParameters().keySet() );
        fromRequest.addAll( toNameSet( request.getRequestParameters() ) );
        
        final Set< String > required = new HashSet<>( m_requiredBeanProperties );
        required.addAll( toNameSet( m_requiredRequestParameters ) );
        
        final Set< String > all = new HashSet<>( required );
        all.addAll( m_optionalBeanProperties );
        all.addAll( toNameSet( m_optionalRequestParameters ) );
        
        final Set< String > retval = new HashSet<>();
        for ( final String r : required )
        {
            if ( !fromRequest.contains( r ) )
            {
                retval.add( "Required parameter missing: " + r );
            }
        }
        for ( final String r : fromRequest )
        {
            if ( !all.contains( r ) )
            {
                retval.add( "Parameter unknown: " + r );
            }
        }
        return retval;
    }
    
    
    private static Set< String > toNameSet( final Set< RequestParameterType > requestParameters )
    {
        final NamingConventionType convention = NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE;
        final Set< String > all = new HashSet<>();
        for ( final RequestParameterType requestParameter : requestParameters )
        {
            all.add( convention.convert( requestParameter.toString() ) );
        }
        return all;
    }


    public String getRequirementDescription()
    {
        final List< String > requiredParameters = new ArrayList<>( m_requiredBeanProperties );
        for ( final RequestParameterType requestParameterType : m_requiredRequestParameters )
        {
            requiredParameters.add(
                    NamingConventionType.UNDERSCORED.convert( requestParameterType.toString() ) );
        }
        
        final List< String > optionalParameters = new ArrayList<>( m_optionalBeanProperties );
        for ( final RequestParameterType requestParameterType : m_optionalRequestParameters )
        {
            optionalParameters.add(
                    NamingConventionType.UNDERSCORED.convert( requestParameterType.toString() ) );
        }
        
        Collections.sort( requiredParameters );
        Collections.sort( optionalParameters );
        
        return "Query Parameters Required: " + requiredParameters + ", Optional: " + optionalParameters;
    }
    
    
    public QueryStringRequirementImpl registerOptionalRequestParameters(
            final RequestParameterType ... requestParameters )
    {
        Validations.verifyNotNull( "Request parameters", requestParameters );
        for ( final RequestParameterType requestParameter: requestParameters )
        {
            registerOptionalRequestParameter( requestParameter );
        }
        return this;
    }
    
    
    private void registerOptionalRequestParameter( final RequestParameterType requestParameter )
    {
        Validations.verifyNotNull( "Request parameter", requestParameter );
        m_optionalRequestParameters.add( requestParameter );
    }
    
    
    public QueryStringRequirementImpl registerOptionalBeanProperties( final String ... beanPropertyNames )
    {
        verifyDaoTypeConfigured();
        Validations.verifyNotNull( "Bean property names", beanPropertyNames );
        for ( final String beanPropertyName : beanPropertyNames )
        {
            registerOptionalBeanProperty( beanPropertyName );
        }
        return this;
    }
    
    
    private void registerOptionalBeanProperty( final String beanPropertyName )
    {
        verifyBeanPropertyHasReader( beanPropertyName );
        m_optionalBeanProperties.add( beanPropertyName );
    }
    
    
    public QueryStringRequirementImpl registerRequiredRequestParameters(
            final RequestParameterType ... requestParameters )
    {
        Validations.verifyNotNull( "Request parameters", requestParameters );
        for ( final RequestParameterType requestParameter: requestParameters )
        {
            registerRequiredRequestParameter( requestParameter );
        }
        return this;
    }
    
    
    private void registerRequiredRequestParameter( final RequestParameterType requestParameter )
    {
        Validations.verifyNotNull( "Request parameter", requestParameter );
        m_requiredRequestParameters.add( requestParameter );
    }
    
    
    public QueryStringRequirementImpl registerRequiredBeanProperties( final String ... beanPropertyNames )
    {
        verifyDaoTypeConfigured();
        Validations.verifyNotNull( "Bean property names", beanPropertyNames );
        for ( final String beanPropertyName : beanPropertyNames )
        {
            registerRequiredBeanProperty( beanPropertyName );
        }
        return this;
    }
    
    
    private void verifyDaoTypeConfigured()
    {
        if ( null == m_daoType )
        {
            throw new IllegalStateException( 
                    "Dao type must be registered to perform the requested operation." );
        }
    }
    
    
    private void registerRequiredBeanProperty( final String beanPropertyName )
    {
        verifyBeanPropertyHasReader( beanPropertyName );
        m_requiredBeanProperties.add( beanPropertyName );
    }
    
    
    private void verifyBeanPropertyHasReader( final String beanPropertyName )
    {
        final Method reader = BeanUtils.getReader( m_daoType, beanPropertyName );
        if ( null == reader )
        {
            throw new RuntimeException(
                    "Property " + m_daoType.getName() + "." + beanPropertyName 
                    + " does not exist or does not have a reader method defined." );
        }
    }
    
    
    public QueryStringRequirement registerDaoType( final Class< ? extends SimpleBeanSafeToProxy > daoType )
    {
        Validations.verifyNotNull( "Dao type", daoType );
        if ( null != m_daoType && daoType != m_daoType )
        {
            throw new IllegalStateException( "Dao type already set to " + m_daoType.getName() + "." );
        }
        m_daoType = daoType;
        return this;
    }
    
    
    public List< RequestHandlerParamContract > getParamsContract( final boolean required )
    {
        final List< RequestHandlerParamContract > retval = new ArrayList<>();
        for ( final RequestParameterType param :
            ( required ) ? m_requiredRequestParameters : m_optionalRequestParameters )
        {
            final RequestHandlerParamContract contract =
                    BeanFactory.newBean( RequestHandlerParamContract.class );
            contract.setName( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.convert(
                    param.toString() ) );
            contract.setType( param.getValueType().getName() );
            retval.add( contract );
        }
        for ( final String param :
            ( required ) ? m_requiredBeanProperties : m_optionalBeanProperties )
        {
            final Method reader = BeanUtils.getReader( 
                    m_daoType, 
                    NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert( param ) );
            if ( null != reader.getAnnotation( ExcludeFromDocumentation.class ) )
            {
                continue;
            }
            
            final RequestHandlerParamContract contract =
                    BeanFactory.newBean( RequestHandlerParamContract.class );
            contract.setName( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.convert( param ) );
            contract.setType( reader.getReturnType().getName() );
            retval.add( contract );
        }
        
        Collections.sort( 
                retval,
                new BeanComparator<>( RequestHandlerParamContract.class, RequestHandlerParamContract.NAME ) );
        
        return retval;
    }


    public Object getBeanSpecifiedViaQueryParameters(
            final CommandExecutionParams params,
            final AutoPopulatePropertiesWithDefaults autoPopulation )
    {
        verifyDaoTypeConfigured();
        final Map< String, String > beanPropertyValues = 
                params.getRequest().getBeanPropertyValueMapFromRequestParameters();
        final Object retval = BeanFactory.newBean( m_daoType );
        if ( AutoPopulatePropertiesWithDefaults.NO == autoPopulation )
        {
            for ( final String prop : BeanUtils.getPropertyNames( m_daoType ) )
            {
                final Method writer = BeanUtils.getWriter( m_daoType, prop );
                try
                {
                    if ( writer.getParameterTypes()[ 0 ].isPrimitive() )
                    {
                        writer.invoke(
                                retval, 
                                MockObjectFactory.objectForType( writer.getParameterTypes()[ 0 ] ) );
                    }
                    else
                    {
                        writer.invoke( retval, new Object [] { null } );
                    }
                }
                catch ( final Exception ex )
                {
                    throw new RuntimeException( "Failed to nullify prop " + prop + ".", ex );
                }
            }
        }
        
        for ( final String prop : m_requiredBeanProperties )
        {
            if ( !beanPropertyValues.containsKey( prop ) )
            {
                throw new S3RestException(
                        AWSFailure.INVALID_ARGUMENT,
                        "Query parameter '" + prop + "' was missing and is required." );
            }
            String value = beanPropertyValues.get( prop );
            if ( null == value 
                    && String.class == BeanUtils.getReader( retval.getClass(), prop ).getReturnType() )
            {
                value = "";
            }            
            populateProperty( params, prop, value, retval );
        }
        for ( final String prop : m_optionalBeanProperties )
        {
            if ( beanPropertyValues.containsKey( prop ) )
            {
                String value = beanPropertyValues.get( prop );
                if ( null == value 
                        && null == BeanUtils.getReader( retval.getClass(), prop ).getAnnotation(
                                Optional.class )
                        && String.class == BeanUtils.getReader( retval.getClass(), prop ).getReturnType() )
                {
                    value = "";
                }           
                populateProperty( params, prop, value, retval );
            }
        }
        
        return retval;
    }
    
    
    private void populateProperty(
            final CommandExecutionParams params,
            final String prop, 
            String value, 
            final Object bean )
    {
        final BeansServiceManager serviceManager = params.getServiceManager();
        final Method writer = BeanUtils.getWriter( bean.getClass(), prop ); 
        try
        {
            final Object typedValue;
            try
            {
                typedValue = MarshalUtil.getTypedValueFromString(
                        writer.getParameterTypes()[ 0 ],
                        value );
            }
            catch ( final RuntimeException ex )
            {
                final Method reader = BeanUtils.getReader( bean.getClass(), prop ); 
                if ( UUID.class == reader.getReturnType() 
                        && null != reader.getAnnotation( References.class ) )
                {
                    final BeansRetriever< ? extends DatabasePersistable > retriever = 
                            serviceManager.getRetriever( reader.getAnnotation( References.class ).value() );
                    final String id;
                    try
                    {
                        id = retriever.discover( value ).getId().toString();
                    }
                    catch ( final RuntimeException ex2 )
                    {
                        throw new S3RestException(
                                GenericFailure.NOT_FOUND,
                                "Could not look up " 
                                + reader.getAnnotation( References.class ).value().getSimpleName()
                                + " via value '" + value + "'.",
                                ex2 );
                    }
                    populateProperty(
                            params,
                            prop, 
                            id,
                            bean );
                    return;
                }
                throw new S3RestException(
                        GenericFailure.BAD_REQUEST,
                        "Value '" + value + "' is not valid for type " 
                        + writer.getParameterTypes()[ 0 ].getSimpleName() + ".",
                        ex );
            }
            
            UserInputValidations.validateUserInput( params.getRequest(), typedValue );
            
            final Class<?>[] paramTypes = writer.getParameterTypes();
            boolean atLeastOneNonPrimitiveSetter = false;
            for(Class< ? > paramType : paramTypes)
            {
                if( !paramType.isPrimitive() )
                {
                    atLeastOneNonPrimitiveSetter = true;
                }
            }
            if ( !atLeastOneNonPrimitiveSetter && null == typedValue )
            {
                final String logMsg = "The property '" + prop + "' ( of type '" 
                        + paramTypes[ 0 ].getSimpleName() + "' ) cannot be null.";                
                LOG.info( logMsg );
                throw new S3RestException( GenericFailure.BAD_REQUEST, logMsg );
            } 
            
            writer.setAccessible( true );
            writer.invoke( 
                    bean, 
                    typedValue );
            return;
        }
        catch ( final Exception ex )
        {
            throw new S3RestException( "Failed to set property " + prop + ".", ex );
        }
    }
    
    
    public String getSampleQueryString(
            final Set< RequestParameterType > requestParametersToExclude,
            final Set< String > beanPropertiesToExclude )
    {
        if ( null != m_daoType )
        {
            for ( final String beanProperty : BeanUtils.getPropertyNames( m_daoType ) )
            {
                final Method reader = BeanUtils.getReader( m_daoType, beanProperty );
                if ( null != reader && null != reader.getAnnotation( ExcludeFromDocumentation.class ) )
                {
                    beanPropertiesToExclude.add( beanProperty );
                }
            }
        }
        
        final StringBuilder queryParameters = new StringBuilder();
        final List< String > requiredParams = new ArrayList<>();
        final List< String > optionalParams = new ArrayList<>();
        final Map< String, String > values = new HashMap<>();
        for ( final RequestParameterType p : m_requiredRequestParameters )
        {
            if ( !requestParametersToExclude.contains( p ) )
            {
                requiredParams.add( p.toString() );
                values.put( p.toString(), getValue( p ) );
            }
        }
        for ( final String p : m_requiredBeanProperties )
        {
            if ( !beanPropertiesToExclude.contains( p ) )
            {
                requiredParams.add( p );
                values.put( p, getValue( p ) );
            }
        }
        for ( final RequestParameterType p : m_optionalRequestParameters )
        {
            if ( !requestParametersToExclude.contains( p ) )
            {
                optionalParams.add( p.toString() );
                values.put( p.toString(), getValue( p ) );
            }
        }
        for ( final String p : m_optionalBeanProperties )
        {
            if ( !beanPropertiesToExclude.contains( p ) )
            {
                optionalParams.add( p );
                values.put( p, getValue( p ) );
            }
        }
        
        Collections.sort( requiredParams );
        Collections.sort( optionalParams );
        
        for ( final String p : requiredParams )
        {
            addHttpQueryParameter( queryParameters, false, p, values.get( p ) );
        }
        for ( final String p : optionalParams )
        {
            addHttpQueryParameter( queryParameters, true, p, values.get( p ) );
        }
        
        return queryParameters.toString();
    }
    
    
    private void addHttpQueryParameter(
            final StringBuilder sb, final boolean optional, String key, final String value )
    {
        key = NamingConventionType.UNDERSCORED.convert( key );
        if ( optional )
        {
            sb.append( "[" );
        }
        if ( sb.toString().contains( "?" ) )
        {
            sb.append( "&" );
        }
        else
        {
            sb.append( "?" );
        }
        sb.append( key );
        if ( null != value )
        {
            sb.append( "={" + value + "}" );
        }
        if ( optional )
        {
            sb.append( "]" );
        }
    }
    
    
    private String getValue( final RequestParameterType requestParameter )
    {
        return getValueInternal( requestParameter.getValueType() );
    }
    
    
    private String getValue( final String beanPropertyName )
    {
        if ( null == m_daoType )
        {
            return getValueInternal( null );
        }
        
        final Method reader = BeanUtils.getReader(
                m_daoType, 
                NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_LOWERCASE.convert( beanPropertyName ) );
        if ( null == reader )
        {
            return getValueInternal( null );
        }
        return getValueInternal( reader.getReturnType() );
    }
    
    
    private String getValueInternal( final Class< ? > valueType )
    {
        if ( null == valueType )
        {
            return "value";
        }
        if ( void.class == valueType )
        {
            return null;
        }
        if ( Date.class.isAssignableFrom( valueType ) )
        {
            return "date";
        }
        if ( String.class.isAssignableFrom( valueType ) )
        {
            return "text";
        }
        if ( UUID.class.isAssignableFrom( valueType ) )
        {
            return "unique identifier or attribute";
        }
        if ( Integer.class.isAssignableFrom( valueType )
                || int.class == valueType )
        {
            return "32-bit integer";
        }
        if ( Long.class.isAssignableFrom( valueType )
                || long.class == valueType )
        {
            return "64-bit integer";
        }
        if ( Double.class.isAssignableFrom( valueType )
                || double.class == valueType )
        {
            return "double";
        }
        if ( Boolean.class.isAssignableFrom( valueType )
                || boolean.class == valueType )
        {
            return "true|false";
        }
        if ( valueType.isEnum() )
        {
            final StringBuilder retval = new StringBuilder();
            for ( final Object e : valueType.getEnumConstants() )
            {
                if ( 0 < retval.length() )
                {
                    retval.append( "|" );
                }
                retval.append( e.toString() );
            }
            return retval.toString();
        }
        throw new UnsupportedOperationException( "No code for " + valueType + "." );
    }

    
    private volatile Class< ? extends SimpleBeanSafeToProxy > m_daoType;
    
    private final Set< RequestParameterType > m_optionalRequestParameters = new HashSet<>();
    private final Set< RequestParameterType > m_requiredRequestParameters = new HashSet<>();
    private final Set< String > m_optionalBeanProperties = new HashSet<>();
    private final Set< String > m_requiredBeanProperties = new HashSet<>();
    private final static Logger LOG = Logger.getLogger( QueryStringRequirementImpl.class );
}
