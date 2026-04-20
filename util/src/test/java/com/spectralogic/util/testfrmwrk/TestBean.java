/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;

import java.util.Date;
import java.util.List;
import java.util.Set;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;
import com.spectralogic.util.notification.domain.NotificationPayload;

public interface TestBean extends SimpleBeanSafeToProxy, NotificationPayload
{
    @ExcludeFromMarshaler( When.ALWAYS )
    @Optional
    Date getNotificationGenerationDate();
    
    String STRING_PROP = "stringProp";
    String getStringProp();
    TestBean setStringProp( final String value );
    
    String INT_PROP = "intProp";
    int getIntProp();
    TestBean setIntProp( final int value );
    
    String OBJECT_INT_PROP = "objectIntProp";
    Integer getObjectIntProp();
    void setObjectIntProp( final Integer value );
    
    String LONG_PROP = "longProp";
    long getLongProp();
    void setLongProp( final long value );
    
    String OBJECT_LONG_PROP = "objectLongProp";
    Long getObjectLongProp();
    void setObjectLongProp( final Long value );
    
    String BOOLEAN_PROP = "booleanProp";
    boolean getBooleanProp();
    void setBooleanProp( final boolean value );
    
    String OBJECT_BOOLEAN_PROP = "objectBooleanProp";
    Boolean getObjectBooleanProp();
    void setObjectBooleanProp( final Boolean value );
    
    String SET_PROP = "setProp";
    @Optional
    Set< String > getSetProp();
    void setSetProp( final Set< String > value );
    
    String LIST_PROP = "listProp";
    @Optional
    List< String > getListProp();
    void setListProp( final List< String > value );
    
    String ARRAY_PROP = "arrayProp";
    @Optional
    String [] getArrayProp();
    void setArrayProp( final String [] value );
    
    String NESTED_BEAN = "nestedBean";
    @Optional
    TestBean getNestedBean();
    void setNestedBean( final TestBean value );
}
