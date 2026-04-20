package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.marshal.CustomMarshaledTypeName;

@CustomMarshaledTypeName( "TapeStateSummary" )
public interface TapeStateSummaryApiBean extends SimpleBeanSafeToProxy {

    String TAPE_STATE = "tapeState";

    TapeState getTapeState();

    void setTapeState(final TapeState value);


    String FULL_OF_DATA = "fullOfData";

    int getFullOfData();

    void setFullOfData(final int value);


    String TYPE_COUNTS = "typeCounts";

    TypeTypeCountApiBean[] getTypeCounts();

    void setTypeCounts( final TypeTypeCountApiBean[] value );

    String COUNT = "count";

    int getCount();

    void setCount( final int value );
}