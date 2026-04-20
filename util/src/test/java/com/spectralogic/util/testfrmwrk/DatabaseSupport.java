/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;

import java.io.File;

import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.manager.DataSource;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;

public interface DatabaseSupport
{
    DataManager getDataManager();
    
    DataSource getDataSource();
    
    DataSource newDataSource();
    
    BeansServiceManager getServiceManager();
    
    BasicTestsInvocationHandler getNotificationEventDispatcherBtih();
    
    String getDbServerName();
    
    String getDbName();
    
    String getDbUsername();
    
    String getDbPassword();
    
    void reset();
    
    void executeSql( final File sqlFile );
}
