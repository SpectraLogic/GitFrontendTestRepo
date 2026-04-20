/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.frmwk;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.UserIdObservable;
import com.spectralogic.s3.server.handler.auth.AuthenticationStrategy;
import com.spectralogic.s3.server.handler.canhandledeterminer.QueryStringRequirement.AutoPopulatePropertiesWithDefaults;
import com.spectralogic.s3.server.handler.canhandledeterminer.RestfulCanHandleRequestDeterminer;
import com.spectralogic.s3.server.handler.command.api.CommandExecutionParams;
import com.spectralogic.s3.server.request.api.DS3Request;
import com.spectralogic.s3.server.request.rest.RestActionType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.servlet.BeanServlet;
import com.spectralogic.s3.server.servlet.api.ServletResponseStrategy;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeansRetriever;

/**
 * A request handler that will create a bean of the type specified.  <br><br>
 * 
 * Any bean properties registered that are indeed bean properties of the dao type this request handler is for
 * will be used to create the targeted bean.  For example, if property Pet.type has a value of 'DOG', then
 * the bean created in the request will have its type property initialized to 'DOG'.
 */
public abstract class BaseCreateBeanRequestHandler< T extends SimpleBeanSafeToProxy & Identifiable >
    extends BaseDaoTypedRequestHandler< T >
{
    protected BaseCreateBeanRequestHandler(
            final Class< T > daoType,
            final AuthenticationStrategy authenticationStrategy,
            final RestDomainType restDomainType )
    {
        this( daoType, authenticationStrategy, restDomainType, null );
    }
    
    
    protected BaseCreateBeanRequestHandler(
            final Class< T > daoType,
            final AuthenticationStrategy authenticationStrategy,
            final RestDomainType restDomainType,
            final DefaultUserIdToUserMakingRequest defaultUserIdToUserMakingRequest )
    {
        super( daoType,
               authenticationStrategy, 
               new RestfulCanHandleRequestDeterminer(
                       RestActionType.CREATE,
                       restDomainType ) );
        m_defaultUserIdToUserMakingRequest = defaultUserIdToUserMakingRequest;
    }
    
    
    public enum DefaultUserIdToUserMakingRequest
    {
        YES,
        NO
    }
    
    
    /**
     * Determines automatically (using the {@link Optional} annotation in addition to any default value 
     * annotations) which properties are required and which are optional
     */
    final protected void registerBeanProperties( final String ... userSpecifiableProperties )
    {
        for ( final String prop : userSpecifiableProperties )
        {
            if ( isOptional( prop ) )
            {
                registerOptionalBeanProperties( prop );
            }
            else
            {
                registerRequiredBeanProperties( prop );
            }
        }
    }
    
    
    private boolean isOptional( final String prop )
    {
        final Method reader = BeanUtils.getReader( m_daoType, prop );
        if ( null == reader )
        {
            return true;
        }
        
        for ( final Annotation a : reader.getAnnotations() )
        {
            if ( Optional.class == a.annotationType()
                    || a.annotationType().getSimpleName().contains( "Default" ) )
            {
                return true;
            }
        }
        if ( UserIdObservable.USER_ID.equals( prop ) 
                && DefaultUserIdToUserMakingRequest.YES == m_defaultUserIdToUserMakingRequest )
        {
            return true;
        }
        return false;
    }
    

    @Override
    protected ServletResponseStrategy handleRequestInternal( 
            final DS3Request request, 
            final CommandExecutionParams params )
    {
        final T bean = getBeanSpecifiedViaQueryParameters( params, AutoPopulatePropertiesWithDefaults.YES );
        final boolean userIdPropertyExists =
                ( BeanUtils.getPropertyNames( m_daoType ).contains( UserIdObservable.USER_ID ) );
        final boolean defaultUserIdBehaviorDefined = ( null != m_defaultUserIdToUserMakingRequest );
        if ( userIdPropertyExists && !defaultUserIdBehaviorDefined )
        {
            throw new IllegalStateException( 
                    "Since " + UserIdObservable.USER_ID + " is a registered bean property for " 
                    + getClass().getSimpleName() 
                    + ", a default user id behavior must be configured." );
        }
        if ( !userIdPropertyExists && defaultUserIdBehaviorDefined )
        {
            throw new IllegalStateException( 
                    "Since " + UserIdObservable.USER_ID + " isn't a registered bean property for " 
                    + getClass().getSimpleName() 
                    + ", a default user id behavior cannot be configured (but it is)." );
        }
        if ( userIdPropertyExists )
        {
            try
            {
                if ( DefaultUserIdToUserMakingRequest.YES == m_defaultUserIdToUserMakingRequest 
                        && null == BeanUtils.getReader( m_daoType, UserIdObservable.USER_ID ).invoke( bean ) )
                {
                    final User user = params.getRequest().getAuthorization().getUser();
                    LOG.info( "Since the " + UserIdObservable.USER_ID 
                            + " property wasn't explicitly specified for the "
                            + m_daoType.getClass().getSimpleName() 
                            + " to create, will populate " + UserIdObservable.USER_ID 
                            + " with the user making this request: " 
                            + ( ( null == user ) ? "null" : user.getName() ) );
                    if ( null != user )
                    {
                        BeanUtils.getWriter( m_daoType, "userId" ).invoke(
                                bean,
                                user.getId() );
                    }
                }
            }
            catch ( final Exception ex )
            {
                throw new RuntimeException( ex );
            }
        }
        
        prepareBeanForCreation( bean );
        validateBeanForCreation( params, bean );
        final UUID idOfCreatedBean = createBean( params, bean );
        
        final BeansRetriever< T > retriever = params.getServiceManager().getRetriever( m_daoType );
        return BeanServlet.serviceCreate(
                params, 
                retriever.attain( idOfCreatedBean ) );
    }
    
    
    protected UUID createBean( final CommandExecutionParams params, final T bean )
    {
        final BeanCreator< T > creator = params.getServiceManager().getCreator( m_daoType );
        creator.create( bean );
        return bean.getId();
    }
    
    
    protected void prepareBeanForCreation( @SuppressWarnings( "unused" ) final T bean )
    {
        // do nothing
    }
    
    
    protected void validateBeanForCreation( 
            @SuppressWarnings( "unused" ) final CommandExecutionParams params,
            @SuppressWarnings( "unused" ) final T bean )
    {
        // do nothing
    }
    
    
    private final DefaultUserIdToUserMakingRequest m_defaultUserIdToUserMakingRequest;
}
