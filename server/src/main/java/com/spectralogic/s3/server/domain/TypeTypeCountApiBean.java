package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.CustomMarshaledTypeName;

@CustomMarshaledTypeName( "TypeTypeCount" )
public interface TypeTypeCountApiBean extends SimpleBeanSafeToProxy {

    String TYPE = "type";

    TapeType getType();

    void setType(final TapeType value);


    String COUNT = "count";

    int getCount();

    void setCount(final int value);


    String FULL_OF_DATA = "fullOfData";

    int getFullOfData();

    void setFullOfData(final int value);

}
