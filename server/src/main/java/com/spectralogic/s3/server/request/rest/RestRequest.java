/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.request.rest;

import java.util.UUID;

import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.db.service.api.BeansRetriever;


public interface RestRequest
{
    public boolean isValidRestRequest();
    
    
    public void validate();
    
    
    public RestActionType getAction();
    
    
    public RestDomainType getDomain();
    
    
    public < T extends SimpleBeanSafeToProxy > T getBean( final BeansRetriever< T > retriever );
    
    
    public UUID getId( final BeansRetriever< ? extends Identifiable > retriever );
    
    
    /**
     * <font color = red><b>
     * Avoid using this accessor.  Try to get the id using one of the other methods.
     * </b></font>
     */
    public String getIdAsString();
}
