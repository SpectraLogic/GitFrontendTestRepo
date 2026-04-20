/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.system;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.spectralogic.s3.common.platform.notification.domain.payload.GenericDaoNotificationPayload;
import com.spectralogic.s3.server.handler.canhandledeterminer.RequestHandlerRequestContract;
import com.spectralogic.s3.server.handler.canhandledeterminer.RequestHandlerRequestContract.RequestHandlerParamContract;
import com.spectralogic.s3.server.handler.find.RequestHandlerProvider;
import com.spectralogic.s3.server.handler.reqhandler.RequestHandler;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.s3.server.request.rest.RestResourceType;
import com.spectralogic.util.bean.BeanComparator;
import com.spectralogic.util.bean.BeanComparator.BeanPropertyComparisonSpecifiction;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.ExcludeFromDocumentation;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.bean.lang.SortBy.Direction;
import com.spectralogic.util.find.PackageContentFinder;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.NamingConventionType;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.marshal.CustomMarshaledName;
import com.spectralogic.util.marshal.MarshalUtil;
import com.spectralogic.util.marshal.CustomMarshaledName.CollectionNameRenderingMode;
import com.spectralogic.util.marshal.CustomMarshaledTypeName;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;
import com.spectralogic.util.notification.domain.NotificationPayload;
import com.spectralogic.util.predicate.UnaryPredicate;

final class ApiContractGenerator
{
    ApiContractGenerator( final Properties properties )
    {
        m_responses = generateResponsesMap( properties );
        m_types.add( RestResourceType.class.getName() );
        m_types.add( RestDomainType.class.getName() );
        m_types.add( RestOperationType.class.getName() );
        m_types.add( RestActionType.class.getName() );
        
        final ResponseContract contract = BeanFactory.newBean( ResponseContract.class );
        contract.setRequestHandlers( generateRequestHandlerContracts() );
        contract.setNotificationPayloadTypes( generateNotificationPayloadTypes() );
        contract.setTypes( generateTypesContracts() );

        m_contractContainer = BeanFactory.newBean( ContractContainer.class );
        m_contractContainer.setContract( contract );
        
        if ( !m_collectionProperties.isEmpty() )
        {
            String msg = 
                    m_collectionProperties.size() 
                    + " properties exist that need to be converted over to using arrays, " 
                    + "which don't have runtime generic type erasure problems: ";
            for ( final String e : m_collectionProperties )
            {
                msg += Platform.NEWLINE + e;
            }
            throw new RuntimeException( msg );
        }
    }
    
    
    ContractContainer getContract()
    {
        return m_contractContainer;
    }
    
    
    private Map< String, Map< Integer, Set< String > > > generateResponsesMap( final Properties properties )
    {
        final Set< String > illegalResponseTypes = CollectionFactory.toSet(
                List.class.getName(), ArrayList.class.getName(),
                Set.class.getName(), HashSet.class.getName() );
        
        final Map< String, Map< Integer, Set< String > > > retval = new HashMap<>();
        for ( final Object key : properties.keySet() )
        {
            final String keyAsString = key.toString();
            final int lastPeriodIndex = keyAsString.lastIndexOf( '.' );
            final String leftPart = keyAsString.substring( 0, lastPeriodIndex );
            final String property = keyAsString.substring( lastPeriodIndex + 1 );
            final int secondToLastPeriodIndex = leftPart.lastIndexOf( '.' );
            final String entryNumber = leftPart.substring( secondToLastPeriodIndex + 1 );
            final String requestHandler = keyAsString.substring( 0, secondToLastPeriodIndex );
            if ( !RequestHandlerExampleResponse.HTTP_RESPONSE_CODE.equals( property ) )
            {
                continue;
            }

            final Integer responseCode = Integer.valueOf( properties.getProperty( 
                    requestHandler + "." + entryNumber + "." 
                    + RequestHandlerExampleResponse.HTTP_RESPONSE_CODE ) );
            final String responseType = properties.getProperty( 
                    requestHandler + "." + entryNumber + "."
                    + RequestHandlerExampleResponse.HTTP_RESPONSE_TYPE );
            if ( illegalResponseTypes.contains( responseType ) )
            {
                m_collectionProperties.add( requestHandler + ".{response payload generated}" );
            }
            if ( !retval.containsKey( requestHandler ) )
            {
                retval.put( requestHandler, new HashMap< Integer, Set< String > >() );
            }
            if ( !retval.get( requestHandler ).containsKey( responseCode ) )
            {
                retval.get( requestHandler ).put( responseCode, new HashSet< String >() );
            }
            retval.get( requestHandler ).get( responseCode ).add( responseType );
        }
        
        return retval;
    }
    
    
    private List< RequestHandlerContract > generateRequestHandlerContracts()
    {
        final Map< String, RequestHandlerRequestContract > requestContracts = new HashMap<>();
        for ( final RequestHandler rh : RequestHandlerProvider.getAllRequestHandlers() )
        {
            final RequestHandlerRequestContract contract =
                    rh.getCanHandleRequestDeterminer().getRequestContract();
            requestContracts.put( rh.getClass().getName(), contract );
            for ( final RequestHandlerParamContract pc : contract.getRequiredParams() )
            {
                if ( null != pc.getType() && pc.getType().contains( "." ) )
                {
                    m_types.add( pc.getType() );
                }
            }
            for ( final RequestHandlerParamContract pc : contract.getOptionalParams() )
            {
                if ( null != pc.getType() && pc.getType().contains( "." ) )
                {
                    m_types.add( pc.getType() );
                }
            }
        }
        
        final Set< String > versions = new HashSet<>();
        final Map< String, String > requestHandlerVersions = new HashMap<>();
        for ( final Map.Entry< Class< ? >, String > e 
                : GetRequestHandlersRequestHandler.getRequestHandlerVersions().entrySet() )
        {
            requestHandlerVersions.put( e.getKey().getName(), e.getValue() );
            if ( versions.contains( e.getValue() ) )
            {
                throw new RuntimeException( 
                        "It is illegal for 2 request handlers to have the same version: " + e.getKey() );
            }
            versions.add( e.getValue() );
        }
        
        final String classificationPrefix = RequestHandler.class.getPackage().getName() + ".";
        final List< RequestHandlerContract > retval = new ArrayList<>();
        for ( final Map.Entry< String, Map< Integer, Set< String > > > e : m_responses.entrySet() )
        {
            final RequestHandlerContract rhc = BeanFactory.newBean( RequestHandlerContract.class );
            rhc.setRequest( requestContracts.get( e.getKey() ) );
            rhc.setName( e.getKey() );
            rhc.setVersion( requestHandlerVersions.get( e.getKey() ) );
            final String classificationPlus = e.getKey().substring( classificationPrefix.length() );
            rhc.setClassification( classificationPlus.substring( 0, classificationPlus.indexOf( '.' ) ) );
            
            final List< ResponseCodeContract > rccs = new ArrayList<>();
            for ( final Map.Entry< Integer, Set< String > > ee : e.getValue().entrySet() )
            {
                final ResponseCodeContract rcc = BeanFactory.newBean( ResponseCodeContract.class );
                rcc.setCode( ee.getKey() );
                final List< ResponseTypeContract > rtcs = new ArrayList<>();
                for ( final String type : ee.getValue() )
                {
                    final ResponseTypeContract rtc = BeanFactory.newBean( ResponseTypeContract.class );
                    rtc.setType( type );
                    rtcs.add( rtc );
                    
                    try
                    {
                        final Class< ? > clazz = Class.forName( type );
                        if ( null != clazz.getComponentType() )
                        {
                            rtc.setType( ARRAY );
                            rtc.setComponentType( clazz.getComponentType().getName() );
                            m_types.add( clazz.getComponentType().getName() );
                        }
                    }
                    catch ( final Exception ex )
                    {
                        Validations.verifyNotNull( "Shut up CodePro.", ex );
                    }
                }
                Collections.sort(
                        rtcs,
                        new BeanComparator<>(
                                ResponseTypeContract.class,
                                new BeanPropertyComparisonSpecifiction(
                                        ResponseTypeContract.TYPE,
                                        Direction.ASCENDING,
                                        null ),
                                new BeanPropertyComparisonSpecifiction(
                                        ResponseTypeContract.COMPONENT_TYPE,
                                        Direction.ASCENDING,
                                        null ) ) );
                rcc.setResponseTypes( rtcs );
                rccs.add( rcc );
                m_types.addAll( ee.getValue() );
            }
            Collections.sort( 
                    rccs, 
                    new BeanComparator<>( ResponseCodeContract.class, ResponseCodeContract.CODE ) );
            rhc.setResponseCodes( rccs );
            retval.add( rhc );
        }
        
        Collections.sort( 
                retval, 
                new BeanComparator<>( RequestHandlerContract.class, RequestHandlerContract.NAME ) );
        
        return retval;
    }
    
    
    private List< String > generateNotificationPayloadTypes()
    {
        final List< String > retval = new ArrayList<>();
        final PackageContentFinder finder = new PackageContentFinder(
                GenericDaoNotificationPayload.class.getPackage().getName(), 
                GenericDaoNotificationPayload.class, 
                null );
        final Set< Class< ? > > classes = finder.getClasses( new UnaryPredicate< Class< ? > >()
        {
            public boolean test( final Class< ? > element )
            {
                return NotificationPayload.class.isAssignableFrom( element );
            }
        } );
        for ( final Class< ? > clazz : classes )
        {
            retval.add( clazz.getName() );
            m_types.add( clazz.getName() );
        }
        
        Collections.sort( retval );
        return retval;
    }
    
    
    private List< TypeContract > generateTypesContracts()
    {
        final Map< Class< ? >, TypeContract > retval = new HashMap<>();
        while ( !m_types.isEmpty() )
        {
            final Set< String > types = new HashSet<>( m_types );
            m_processedTypes.addAll( m_types );
            m_types.clear();
            for ( final String typeDiscovered : new HashSet<>( types ) )
            {
                if ( "null".equals( typeDiscovered ) )
                {
                    continue;
                }
                try
                {
                    generateTypesContracts( Class.forName( typeDiscovered ), retval );
                }
                catch ( final ClassNotFoundException ex )
                {
                    throw new RuntimeException( ex );
                }
            }
        }
        
        final List< TypeContract > sortedRetval = new ArrayList<>( retval.values() );
        Collections.sort( 
                sortedRetval, 
                new BeanComparator<>( TypeContract.class, TypeContract.NAME ) );
        return sortedRetval;
    }
    
    
    private void generateTypesContracts( 
            final Class< ? > type, 
            final Map< Class< ? >, TypeContract > contracts )
    {
        if ( !type.getName().startsWith( "com.spectralogic." )
                || contracts.containsKey( type ) )
        {
            return;
        }
        
        final TypeContract contract = BeanFactory.newBean( TypeContract.class );
        if ( null != type.getEnumConstants() )
        {
            final EnumConstantContract [] eccs = new EnumConstantContract[ type.getEnumConstants().length ];
            for ( int i = 0; i < eccs.length; ++i )
            {
                final Object enumConstant = type.getEnumConstants()[ i ];
                eccs[ i ] = BeanFactory.newBean( EnumConstantContract.class );
                eccs[ i ].setName( MarshalUtil.getMarshaledEnumName( enumConstant ) );
                eccs[ i ].setProperties( new ArrayList< AnnotationElementContract >() );
            }
            contract.setEnumConstants( CollectionFactory.toList( eccs ) );
        }
        
        final CustomMarshaledTypeName customTypeName = type.getAnnotation( CustomMarshaledTypeName.class );
        if ( null != customTypeName )
        {
            contract.setNameToMarshal( customTypeName.value() );
        }
        
        contract.setName( type.getName() );
        final List< TypeElementContract > ecs = new ArrayList<>();
        final Set< Class< ? > > nestedTypes = new HashSet<>();
        final Set< String > props = BeanUtils.getPropertyNames( type );
        props.remove( "declaringClass" );
        for ( final String prop : props )
        {
            final Method reader = BeanUtils.getReader( type, prop );
            if ( null == reader 
                    || 0 < reader.getParameterTypes().length 
                    || null != reader.getAnnotation( ExcludeFromDocumentation.class ) )
            {
                continue;
            }
            if ( null != contract.getEnumConstants() )
            {
                for ( int i = 0; i < contract.getEnumConstants().size(); ++i )
                {
                    try
                    {
                        final AnnotationElementContract aec =
                                BeanFactory.newBean( AnnotationElementContract.class );
                        Object v = reader.invoke( type.getEnumConstants()[ i ] );
                        aec.setName( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.convert(
                                prop ) );
                        aec.setValueType( reader.getReturnType().getName() );
                        if ( null != reader.getReturnType().getComponentType() 
                                || ( null != v && Collection.class.isAssignableFrom( v.getClass() ) ) )
                        {
                            if ( null != reader.getReturnType().getComponentType() )
                            {
                                aec.setComponentType( reader.getReturnType().getComponentType().getName() );
                            }
                            aec.setValueType( ARRAY );
                            if ( null != v )
                            {
                                final List< String > list = new ArrayList<>();
                                if ( reader.getReturnType().isArray() )
                                {
                                    for ( int j = 0; j < Array.getLength( v ); ++j )
                                    {
                                        list.add( Array.get( v, j ).toString() );
                                    }
                                }
                                else
                                {
                                    for ( final Object o : (Collection<?>)v )
                                    {
                                        list.add( o.toString() );
                                    }
                                }
                                Collections.sort( list );
                                v = list;
                            }
                        }
                        aec.setValue( ( null == v ) ? "null" : v.toString() );
                        
                        final List< AnnotationElementContract > newProperties = 
                                contract.getEnumConstants().get( i ).getProperties();
                        newProperties.add( aec );
                        Collections.sort(
                                newProperties, 
                                new BeanComparator<>( 
                                        AnnotationElementContract.class, AnnotationElementContract.NAME ) );
                        contract.getEnumConstants().get( i ).setProperties( newProperties );
                    }
                    catch ( final Exception ex )
                    {
                        throw new RuntimeException( 
                                "Failed to invoke " + reader + " on " + type.getEnumConstants()[ i ] + ".", 
                                ex );
                    }
                }
            }
            
            final TypeElementContract ec = BeanFactory.newBean( TypeElementContract.class );
            ec.setName( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.convert( prop ) );
            ec.setType( reader.getReturnType().getName() );
            ec.setComponentType( ( null == reader.getReturnType().getComponentType() ) ? 
                    null 
                    : reader.getReturnType().getComponentType().getName() );
            if ( null != ec.getComponentType() )
            {
                if ( !m_processedTypes.contains( ec.getComponentType() ) )
                {
                    m_types.add( ec.getComponentType() );
                }
                ec.setType( ARRAY );
            }
            else if ( Collection.class.isAssignableFrom( reader.getReturnType() ) 
                    && null == type.getEnumConstants() )
            {
                m_collectionProperties.add( type.getName() + "." + prop );
            }
            final List< AnnotationContract > acs = new ArrayList<>();
            for ( final Annotation annotation : reader.getAnnotations() )
            {
                final Class< ? extends Annotation > clazz = annotation.annotationType();
                final AnnotationContract ac = BeanFactory.newBean( AnnotationContract.class );
                ac.setName( clazz.getName() );
                final List< AnnotationElementContract > aecs = new ArrayList<>();
                for ( final Method m : clazz.getMethods() )
                {
                    if ( 0 < m.getParameterTypes().length 
                            || m.getName().equals( "annotationType" ) 
                            || m.getName().equals( "hashCode" ) 
                            || m.getName().equals( "toString" ) )
                    {
                        continue;
                    }
                    final AnnotationElementContract aec = 
                            BeanFactory.newBean( AnnotationElementContract.class );
                    aec.setName( NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.convert(
                            m.getName() ) );
                    try
                    {
                        final Object value = m.invoke( annotation );
                        aec.setValueType( value.getClass().getName() );
                        if ( Class.class.isAssignableFrom( value.getClass() ) )
                        {
                            aec.setValue( ( (Class<?>)value ).getName() );
                        }
                        else
                        {
                            aec.setValue( value.toString() );
                        }
                    }
                    catch ( final Exception ex )
                    {
                        throw new RuntimeException(
                                "Failed to invoke " + m + " on " + annotation + ".", ex );
                    }
                    aecs.add( aec );
                }
                Collections.sort(
                        aecs, 
                        new BeanComparator<>( 
                                AnnotationElementContract.class, AnnotationElementContract.NAME ) );
                ac.setAnnotationElements( aecs );
                acs.add( ac );
            }
            Collections.sort(
                    acs, 
                    new BeanComparator<>( 
                            AnnotationContract.class, AnnotationContract.NAME ) );
            ec.setAnnotations( acs );
            nestedTypes.add( reader.getReturnType() );
            ecs.add( ec );
        }
        Collections.sort(
                ecs, 
                new BeanComparator<>( TypeElementContract.class, TypeElementContract.NAME ) );
        contract.setElements( ecs );
        contracts.put( type, contract );
        
        for ( final Class< ? > nestedType : nestedTypes )
        {
            generateTypesContracts( nestedType, contracts );
        }
    }
    
    
    interface ContractContainer extends SimpleBeanSafeToProxy
    {
        String CONTRACT = "contract";
        
        ResponseContract getContract();
        
        void setContract( final ResponseContract value );
    } // end inner class def
    
    
    interface ResponseContract extends SimpleBeanSafeToProxy
    {
        String REQUEST_HANDLERS = "requestHandlers";
        
        @CustomMarshaledName( 
                collectionValue = "RequestHandlers", 
                value = "RequestHandler", 
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        List< RequestHandlerContract > getRequestHandlers();
        
        void setRequestHandlers( final List< RequestHandlerContract > value );
        
        
        String TYPES = "types";

        @CustomMarshaledName( 
                collectionValue = "Types", 
                value = "Type", 
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        List< TypeContract > getTypes();
        
        void setTypes( final List< TypeContract > value );
        
        
        String NOTIFICATION_PAYLOAD_TYPES = "notificationPayloadTypes";

        @CustomMarshaledName( 
                collectionValue = "NotificationPayloadTypes", 
                value = "NotificationPayloadType", 
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        List< String > getNotificationPayloadTypes();
        
        void setNotificationPayloadTypes( final List< String > value );
    } // end inner class def
    
    
    interface RequestHandlerContract extends SimpleBeanSafeToProxy
    {
        String NAME = "name";
        
        @MarshalXmlAsAttribute
        String getName();
        
        void setName( final String value );
        
        
        String CLASSIFICATION = "classification";
        
        @MarshalXmlAsAttribute
        String getClassification();
        
        void setClassification( final String value );
        
        
        String VERSION = "version";
        
        String getVersion();
        
        void setVersion( final String value );
        
        
        String REQUEST = "request";
        
        RequestHandlerRequestContract getRequest();
        
        void setRequest( final RequestHandlerRequestContract value );
        
        
        String RESPONSE_CODES = "responseCodes";

        @CustomMarshaledName( 
                collectionValue = "ResponseCodes", 
                value = "ResponseCode", 
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        List< ResponseCodeContract > getResponseCodes();
        
        void setResponseCodes( final List< ResponseCodeContract > value );
    } // end inner class def
    
    
    interface ResponseCodeContract extends SimpleBeanSafeToProxy
    {
        String CODE = "code";

        Integer getCode();
        
        void setCode( final Integer value );
        
        
        String RESPONSE_TYPES = "responseTypes";

        @CustomMarshaledName( 
                collectionValue = "ResponseTypes", 
                value = "ResponseType", 
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        List< ResponseTypeContract > getResponseTypes();
        
        void setResponseTypes( final List< ResponseTypeContract > value );
    } // end inner class def
    
    
    interface ResponseTypeContract extends SimpleBeanSafeToProxy
    {
        String TYPE = "type";

        @MarshalXmlAsAttribute
        String getType();
        
        void setType( final String value );
        

        String COMPONENT_TYPE = "componentType";
        
        @ExcludeFromMarshaler( When.VALUE_IS_NULL )
        @MarshalXmlAsAttribute
        String getComponentType();
        
        void setComponentType( final String value );
    } // end inner class def
    
    
    interface TypeContract extends SimpleBeanSafeToProxy
    {
        String NAME = "name";

        @MarshalXmlAsAttribute
        String getName();
        
        void setName( final String value );
        
        
        String NAME_TO_MARSHAL = "nameToMarshal";

        @ExcludeFromMarshaler( When.VALUE_IS_NULL )
        @MarshalXmlAsAttribute
        String getNameToMarshal();
        
        void setNameToMarshal( final String value );
        
        
        String ELEMENTS = "elements";

        @CustomMarshaledName( 
                collectionValue = "Elements", 
                value = "Element", 
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        List< TypeElementContract > getElements();
        
        void setElements( final List< TypeElementContract > value );
        
        
        String ENUM_CONSTANTS = "enumConstants";

        @ExcludeFromMarshaler( When.VALUE_IS_NULL )
        @CustomMarshaledName( 
                collectionValue = "EnumConstants", 
                value = "EnumConstant", 
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        List< EnumConstantContract > getEnumConstants();
        
        void setEnumConstants( final List< EnumConstantContract > value );
    } // end inner class def
    
    
    interface EnumConstantContract extends SimpleBeanSafeToProxy
    {
        String NAME = "name";

        @MarshalXmlAsAttribute
        String getName();
        
        void setName( final String value );
        
        
        String PROPERTIES = "properties";

        @ExcludeFromMarshaler( When.VALUE_IS_NULL )
        @CustomMarshaledName( 
                collectionValue = "Properties", 
                value = "Property", 
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        List< AnnotationElementContract > getProperties();
        
        void setProperties( final List< AnnotationElementContract > value );
    } // end inner class def
    
    
    interface TypeElementContract extends SimpleBeanSafeToProxy
    {
        String NAME = "name";

        @MarshalXmlAsAttribute
        String getName();
        
        void setName( final String value );
        
        
        String ANNOTATIONS = "annotations";

        @CustomMarshaledName( 
                collectionValue = "Annotations", 
                value = "Annotation", 
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        List< AnnotationContract > getAnnotations();
        
        void setAnnotations( final List< AnnotationContract > value );
        
        
        String TYPE = "type";
        
        @MarshalXmlAsAttribute
        String getType();
        
        void setType( final String value );
        
        
        String COMPONENT_TYPE = "componentType";
        
        @ExcludeFromMarshaler( When.VALUE_IS_NULL )
        @MarshalXmlAsAttribute
        String getComponentType();
        
        void setComponentType( final String value );
    } // end inner class def
    
    
    interface AnnotationContract extends SimpleBeanSafeToProxy
    {
        String NAME = "name";

        @MarshalXmlAsAttribute
        String getName();
        
        void setName( final String value );
        
        
        String ANNOTATION_ELEMENTS = "annotationElements";

        @CustomMarshaledName( 
                collectionValue = "AnnotationElements", 
                value = "AnnotationElement", 
                collectionValueRenderingMode = CollectionNameRenderingMode.SINGLE_BLOCK_FOR_ALL_ELEMENTS )
        List< AnnotationElementContract > getAnnotationElements();
        
        void setAnnotationElements( final List< AnnotationElementContract > values );
    } // end inner class def
    
    
    interface AnnotationElementContract extends SimpleBeanSafeToProxy
    {
        String NAME = "name";

        @MarshalXmlAsAttribute
        String getName();
        
        AnnotationElementContract setName( final String value );
        
        
        String VALUE = "value";

        @MarshalXmlAsAttribute
        String getValue();
        
        AnnotationElementContract setValue( final String value );
        
        
        String VALUE_TYPE = "valueType";

        @MarshalXmlAsAttribute
        String getValueType();
        
        AnnotationElementContract setValueType( final String value );
        
        
        String COMPONENT_TYPE = "componentType";
        
        @ExcludeFromMarshaler( When.VALUE_IS_NULL )
        @MarshalXmlAsAttribute
        String getComponentType();
        
        void setComponentType( final String value );
    } // end inner class def
    
    
    private final ContractContainer m_contractContainer;
    private final Map< String, Map< Integer, Set< String > > > m_responses;
    private final Set< String > m_types = new HashSet<>();
    private final Set< String > m_processedTypes = new HashSet<>();
    private final Set< String > m_collectionProperties = new HashSet<>();
    
    private final static String ARRAY = "array";
}
