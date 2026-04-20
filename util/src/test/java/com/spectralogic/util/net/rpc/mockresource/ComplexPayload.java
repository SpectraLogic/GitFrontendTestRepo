/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.mockresource;

import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.ExcludeFromMarshaler;
import com.spectralogic.util.marshal.ExcludeFromMarshaler.When;

public interface ComplexPayload extends SimpleBeanSafeToProxy
{
    String NESTED1 = "nested1";
    
    @Optional
    ComplexPayload getNested1();
    
    void setNested1( final ComplexPayload value );
    

    String NESTED2 = "nested2";
    
    @Optional
    ComplexPayload [] getNested2();
    
    void setNested2( final ComplexPayload [] value );
    

    String NESTED3 = "nested3";
    
    @Optional
    ComplexPayload [] getNested3();
    
    void setNested3( final ComplexPayload [] value );
    

    String NESTED4 = "nested4";
    
    @Optional
    @ExcludeFromMarshaler( When.VALUE_IS_NULL )
    ComplexPayload [] getNested4();
    
    void setNested4( final ComplexPayload [] value );
    
    
    String NESTED5 = "nested5";
    
    String [] getNested5();
    
    void setNested5( final String [] value );
}
