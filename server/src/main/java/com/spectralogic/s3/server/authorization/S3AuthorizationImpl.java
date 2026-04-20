/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.authorization;

import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.authorization.api.AuthorizationValidationStrategy;
import com.spectralogic.s3.server.authorization.api.S3Authorization;
import com.spectralogic.util.http.HttpRequest;
import com.spectralogic.util.lang.Validations;

public final class S3AuthorizationImpl implements S3Authorization
{
    public S3AuthorizationImpl( final HttpRequest httpRequest )
    {
        Validations.verifyNotNull( "Request", httpRequest );
        m_httpRequest = httpRequest;

        final String authorization = m_httpRequest.getHeader(
                S3HeaderType.AUTHORIZATION );
        if ( null == authorization || authorization.trim().isEmpty() )
        {
            m_id = null;
            m_signature = null;
        }
        else
        {
            StringBuilder sb = new StringBuilder();
            int  i= 0;
            if (authorization.startsWith("AWS "))
            {
                i += 4;
            }
            for (;i < authorization.length(); i++)
            {
                char c = authorization.charAt(i);

                if (c == ':')
                {
                    i++;
                    break;
                }

                sb.append(c);
            }
            m_id = sb.toString();
            sb = new StringBuilder();
            for (;i < authorization.length(); i++)
            {
                sb.append(authorization.charAt(i));
            }
            m_signature = ( 0 == sb.length() ) ? null : sb.toString();
        }
    }

    
    public String getId()
    {
        return m_id;
    }

    
    public User getUser()
    {
        if ( !m_validated )
        {
            throw new IllegalStateException( "Validation must occur before you can get the owner." );
        }
        return m_user;
    }
    
    
    public UUID getUserId()
    {
        final User user = getUser();
        return ( null == user ) ? null : user.getId();
    }

    
    public void validate( final AuthorizationValidationStrategy strategy )
    {
        Validations.verifyNotNull( "Strategy", strategy );
        m_user = strategy.getAuthorization( m_httpRequest, m_id, m_signature );
        m_validated = true;
        LOG.info( 
            "Authorization is valid.  User is '" 
            + ( (null != m_user) ? m_user.getName() : "ANONYMOUS_LOGON" )
            + "'." );
    }
    
    
    private volatile boolean m_validated;
    private volatile User m_user;
    private final String m_id;
    private final String m_signature;
    private final HttpRequest m_httpRequest;

    private static final Logger LOG = Logger.getLogger(S3AuthorizationImpl.class);
}
