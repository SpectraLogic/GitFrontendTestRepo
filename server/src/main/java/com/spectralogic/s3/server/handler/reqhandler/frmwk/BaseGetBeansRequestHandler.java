/*
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.server.handler.reqhandler.frmwk;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.AuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.CollectionFactory;
import static com.spectralogic.s3.server.handler.reqhandler.frmwk.FillResultSetUtil.fillResultSet;

/**
 * A request handler that will return all beans of a particular type.  <br><br>
 * 
 * Any bean properties registered that are indeed bean properties of the dao type this request handler is for
 * will be used as filters for the response.  For example, if property Pet.type has a value of 'DOG', then
 * only pets of type 'DOG' will be returned.
 */
public abstract class BaseGetBeansRequestHandler< T extends SimpleBeanSafeToProxy & Identifiable > 
    extends BaseDaoTypedRequestHandler< T >
{
    
    
    protected BaseGetBeansRequestHandler(
            final Class< T > daoType,
            final AuthenticationStrategy authenticationStrategy,
            final RestDomainType restDomain )
    {
        super( daoType,
               authenticationStrategy,
               new RestfulCanHandleRequestDeterminer( RestActionType.LIST, restDomain ) );
        
        registerOptionalRequestParameters(
                RequestParameterType.PAGE_LENGTH,
                RequestParameterType.PAGE_OFFSET,
                RequestParameterType.LAST_PAGE,
                RequestParameterType.PAGE_START_MARKER );
    }
    
    
    @Override final protected ServletResponseStrategy handleRequestInternal(
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        final List< T > beans = generateResponse( request, params );
    
        final List< T > initialRetval = beans.parallelStream()
                                             .map( bean -> performCustomPopulationWork( request, params, bean ) )
                                             .collect( Collectors.toList() );
        List< ? extends T > retval = performCustomPopulationWork( request, params, new ArrayList<>( initialRetval ) );
    
        return BeanServlet.serviceGet( params, CollectionFactory.toArray( m_daoType, retval ) );
    }
    
    
    private List< T > generateResponse( final DS3Request request, final CommandExecutionParams params )
    {
        final BeansRetriever< T > retriever = params.getServiceManager()
                                                    .getRetriever( m_daoType );
        final Set< String > propsToFilterBy = request.getBeanPropertyValueMapFromRequestParameters()
                                                     .keySet();
        final List< WhereClause > filters = getWhereClauses( params, propsToFilterBy );
        final RequestPagingProperties pagingProperties =
                new RequestPagingProperties( request, retriever.getCount( Require.all( filters ) ) );
        final RequestSortingProperties sortingProperties = new RequestSortingProperties( request );
        final List< T > resultSet = new ArrayList<>();
        int remainingRecordCount = fillResultSet( filters, retriever, pagingProperties, sortingProperties, resultSet );
    
        request.getHttpResponse()
               .addHeader( S3HeaderType.PAGING_TOTAL_RESULT_COUNT.getHttpHeaderName(),
                       String.valueOf( pagingProperties.getTotalRecordCount() ) );
        request.getHttpResponse()
               .addHeader( S3HeaderType.PAGING_TRUNCATED.getHttpHeaderName(), String.valueOf( remainingRecordCount ) );
    
        return resultSet;
    }
    
    
    private List< WhereClause > getWhereClauses( final CommandExecutionParams params,
            final Set< String > propsToFilterBy )
    {
        final List< WhereClause > filters = new ArrayList<>();
        final T requestBean = getBeanSpecifiedViaQueryParameters( params, AutoPopulatePropertiesWithDefaults.NO );
        filters.add( getCustomFilter( requestBean, params ) );
        for ( final String prop : propsToFilterBy )
        {
            final Method reader = BeanUtils.getReader( m_daoType, prop );
            if ( null == reader )
            {
                LOG.info( "Failed to find a reader on " + m_daoType.getName() + " for property " + prop +
                        ".  Will not consider it when filtering the results." );
                continue;
            }
            
            try
            {
                final Object value = reader.invoke( requestBean );
                if ( String.class == reader.getReturnType() && null != value )
                {
                    final boolean delimiterAnd = value.toString().contains( AND_DELIMITER );
                    final boolean delimiterOr = value.toString().contains( OR_DELIMITER );
                    if ( delimiterAnd && delimiterOr )
                    {
                        throw new S3RestException( GenericFailure.BAD_REQUEST,
                                "Cannot specify both " + AND_DELIMITER + " and " + OR_DELIMITER + "." );
                    }
                    else if ( delimiterAnd )
                    {
                        for ( final String v : value.toString().split( Pattern.quote( AND_DELIMITER ) ) )
                        {
                            filters.add( Require.beanPropertyMatchesInsensitive( prop, v ) );
                        }
                    }
                    else if ( delimiterOr )
                    {
                        final Set< WhereClause > ors = new HashSet<>();
                        for ( final String v : value.toString().split( Pattern.quote( OR_DELIMITER ) ) )
                        {
                            ors.add( Require.beanPropertyMatchesInsensitive( prop, v ) );
                        }
                        filters.add( Require.any( ors ) );
                    }
                    else
                    {
                        filters.add( Require.beanPropertyMatchesInsensitive( prop, value.toString() ) );
                    }
                }
                else
                {
                    filters.add( Require.beanPropertyEquals( prop, value ) );
                }
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( "Failed to add filter for " + prop + ".", ex );
            }
        }
        return filters;
    }
    
    
    protected WhereClause getCustomFilter(
            @SuppressWarnings( "unused" ) final T requestBean,
            @SuppressWarnings( "unused" ) final CommandExecutionParams params )
    {
        return Require.nothing();
    }
    
    
    protected List< ? extends T > performCustomPopulationWork(
            @SuppressWarnings( "unused" ) final DS3Request request,
            @SuppressWarnings( "unused" ) final CommandExecutionParams params,
            final List< T > beans )
    {
        return beans;
    }
    
    
    protected T performCustomPopulationWork(
            @SuppressWarnings( "unused" ) final DS3Request request,
            @SuppressWarnings( "unused" ) final CommandExecutionParams params,
            final T bean )
    {
        return bean;
    }
    
    
    /** The delimiter to use in String query filters to break up a single query filter into multiple */
    public final static String AND_DELIMITER = "!#AND#!";
    public final static String OR_DELIMITER = "!#OR#!";
}
