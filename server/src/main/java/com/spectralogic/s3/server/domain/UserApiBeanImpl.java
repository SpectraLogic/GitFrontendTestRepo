/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.domain;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.util.marshal.BaseMarshalable;
import com.spectralogic.util.marshal.CustomMarshaledName;

final public class UserApiBeanImpl extends BaseMarshalable implements UserApiBean
{
    UserApiBeanImpl()
    {
        // empty
    }
    
    
    static public UserApiBean fromUser( final User user )
    {
        return new UserApiBeanImpl().setDisplayName( user.getName() ).setId( user.getId() );
    }
 
    
    @CustomMarshaledName( "iD" )
    public UUID getId()
    {
        return m_id;
    }
    
    public UserApiBean setId( final UUID value )
    {
        m_id = value;
        return this;
    }
    
        
    public String getDisplayName()
    {
        return m_displayName;
    }
    
    public UserApiBean setDisplayName( final String value )
    {
        m_displayName = value;
        return this;
    }
    
    
    private volatile UUID m_id;
    private volatile String m_displayName;
}