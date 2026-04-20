package com.spectralogic.s3.server.domain;

import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.util.marshal.ExcludeDefaultsFromMarshaler;
import com.spectralogic.util.marshal.MarshalXmlAsAttribute;

@ExcludeDefaultsFromMarshaler
public interface TapePartitionApiBean extends TapePartition {

    @MarshalXmlAsAttribute
    String getName();


    String TAPE_COUNT = "tapeCount";

    int getTapeCount();

    void setTapeCount(final int tapeCount);


    String DRIVE_COUNT = "driveCount";

    int getDriveCount();

    void setDriveCount(final int driveCount);
}
