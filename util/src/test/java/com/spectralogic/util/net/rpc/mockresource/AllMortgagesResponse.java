/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.mockresource;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface AllMortgagesResponse extends SimpleBeanSafeToProxy
{
    String MORTGAGES = "mortgages";
    
    Mortgage [] getMortgages();
    
    void setMortgages( final Mortgage [] value );
}