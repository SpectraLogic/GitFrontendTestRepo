package com.spectralogic.util.marshal;

import com.spectralogic.util.bean.BeanMethodInvocationHandler;
import com.spectralogic.util.lang.NamingConventionType;

public interface Marshalable 
{
    /**
     * @return JSON representation of the object using the naming convention specified
     */
    @BeanMethodInvocationHandler( ToJsonInvocationHandler.class )
    public String toJson( final NamingConventionType namingConvention );
    
    /**
     * @return JSON representation of the object using the default naming convention
     */
    @BeanMethodInvocationHandler( ToJsonInvocationHandler.class )
    public String toJson();
    
    /**
     * @return XML representation of the object using the naming convention specified
     */
    @BeanMethodInvocationHandler( ToXmlInvocationHandler.class )
    public String toXml( final NamingConventionType namingConvention );
    
    /**
     * @return XML representation of the object using the default naming convention
     */
    @BeanMethodInvocationHandler( ToXmlInvocationHandler.class )
    public String toXml();
}
