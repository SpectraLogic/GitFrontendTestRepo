/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.authorization;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.platform.aws.AWSFailure;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.platform.aws.security.V2AuthorizationSignatureValidator;
import com.spectralogic.s3.server.authorization.api.AuthorizationValidationStrategy;
import com.spectralogic.s3.server.exception.S3RestException;
import com.spectralogic.s3.server.handler.auth.InternalAccessOnlyAuthenticationStrategy;
import com.spectralogic.util.db.service.api.BeansRetriever;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.http.HttpRequest;
import com.spectralogic.util.lang.Validations;

/**
 * Validation strategy that ensures that the authorization header is either:
 * <br><br>
 *  (i) Not provided, or
 *  (ii) valid (the secret provided matches the username represented by the authorization id)
 */
public final class AuthorizationValidationStrategyImpl implements AuthorizationValidationStrategy
{
    public AuthorizationValidationStrategyImpl( final BeansRetriever< User > userRetriever )
    {
        Validations.verifyNotNull( "User retriever", userRetriever );
        m_userRetriever = userRetriever;
    }
    
    
    /**
     * @return OwnerBean if there is an auth id and the credentials sent are valid, null if there
     * is no auth id to authenticate, or throws an exception if an auth id is provided but the
     * credentials are invalid
     */
    public User getAuthorization( 
            final HttpRequest httpRequest, 
            final String authId,
            final String sentDigest )
    {
        if ( InternalAccessOnlyAuthenticationStrategy.isRequest(
                S3HeaderType.IMPERSONATE_USER, httpRequest ) )
        {
            final String userToImpersonate = httpRequest.getHeader(
                    S3HeaderType.IMPERSONATE_USER );
            LOG.info( "User impersonation allowed for '" + userToImpersonate + "'." );
            return m_userRetriever.discover( userToImpersonate );
        }
        
        if ( authId == null )
        {
            return null;
        }

        if ( sentDigest == null )
        {
            throw new S3RestException(
                    AWSFailure.MISSING_SECURITY_HEADER,
                    "The authorization signature is missing from the " 
                    + S3HeaderType.AUTHORIZATION.getHttpHeaderName() + " HTTP header." );
        }

        
        /*
         * We don't want somebody to be able to brute force attack passwords, so only allow one in-process
         * authentication at a time and block the lock for a bit if authentication fails.
         */
        final User user = getUserFromAuthid(authId);
        synchronized ( this )
        {
            if ( user == null )
            {
                throw new S3RestException( 
                        AWSFailure.INVALID_ACCESS_KEY_ID, 
                        "Authorization id '" + authId + "' is unknown." );
            }
            
            for ( final String headerName : httpRequest.getHeaders().keySet() )
            {
                if ( headerName.equalsIgnoreCase( 
                        S3HeaderType.DISABLE_AUTHORIZATION_HEADER_VERIFICATION
                        .getHttpHeaderName() ) )
                {
                    m_internalAccessOnlyAuthenticationStrategy.authenticate( httpRequest );
                    return user;
                }
            }
            
            try
            {
                new V2AuthorizationSignatureValidator( 
                        httpRequest, user.getSecretKey() ).validate( sentDigest );
            }
            catch ( final Exception ex )
            {
                if ( FailureTypeObservableException.class.isAssignableFrom( ex.getClass() ) )
                {
                    throw (FailureTypeObservableException)ex;
                }
                throw new S3RestException( 
                        AWSFailure.INVALID_SECURITY, 
                        "Authorization signature is invalid.", ex );
            }
        }
        
        return user;
    }
    

    private User getUserFromAuthid(final String authId)
    {
        if (m_userRetriever == null)
        {
            throw new IllegalStateException( "Metadata store not set yet." );
        }

        return m_userRetriever.retrieve( User.AUTH_ID, authId );
    }
    
    
    private final BeansRetriever< User > m_userRetriever;
    private final InternalAccessOnlyAuthenticationStrategy m_internalAccessOnlyAuthenticationStrategy =
            new InternalAccessOnlyAuthenticationStrategy();
    private final static Logger LOG = Logger.getLogger( AuthorizationValidationStrategyImpl.class );
}
