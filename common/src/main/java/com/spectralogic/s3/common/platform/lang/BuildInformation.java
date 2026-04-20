/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.platform.lang;

import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;

public interface BuildInformation extends SimpleBeanSafeToProxy
{
    String VERSION = "version";
    
    String getVersion();
    
    void setVersion( final String value );
    
    
    String REVISION = "revision";
    
    String getRevision();
    
    void setRevision( final String value );
    
    
    String BRANCH = "branch";
    
    String getBranch();
    
    void setBranch( final String value );
}
