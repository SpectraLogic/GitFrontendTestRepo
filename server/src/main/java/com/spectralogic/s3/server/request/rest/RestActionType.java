/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.request.rest;

import com.spectralogic.util.http.RequestType;

public enum RestActionType
{
    /**
     * Delete all of a particular type
     */
    BULK_DELETE( RequestType.DELETE, false ),
    
    
    /**
     * Modify all of a particular type
     */
    BULK_MODIFY( RequestType.PUT, false ),
    
    
    /**
     * Create one of a particular type
     */
    CREATE( RequestType.POST, false ),
    
    
    /**
     * Delete one of a particular type
     */
    DELETE( RequestType.DELETE, true ),
    
    
    /**
     * List all of a particular type
     */
    LIST( RequestType.GET, false ),
    
    
    /**
     * Modify one of a particular type
     */
    MODIFY( RequestType.PUT, true ),
    
    
    /**
     * Show one of a particular type
     */
    SHOW( RequestType.GET, true ),
    
    ;
    
    
    private RestActionType( final RequestType defaultHttpVerb, final boolean idApplies )
    {
        m_defaultHttpVerb = defaultHttpVerb;
        m_idApplies = idApplies;
    }
    
    
    public RequestType getDefaultHttpVerb()
    {
        return m_defaultHttpVerb;
    }
    
    
    public boolean isIdApplicable()
    {
        return m_idApplies;
    }
    
    
    private final RequestType m_defaultHttpVerb;
    private final boolean m_idApplies;
}
