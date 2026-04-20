package com.spectralogic.util.notification.domain;

import com.spectralogic.util.bean.lang.AutoIncrementing;
import com.spectralogic.util.db.lang.DatabasePersistable;

public interface SequencedEvent<T extends SequencedEvent<T>> extends DatabasePersistable {

    String SEQUENCE_NUMBER = "sequenceNumber";

    @AutoIncrementing
    Long getSequenceNumber();

    //NOTE: should only be used in test - the value is auto-incrementing
    T setSequenceNumber(final Long value);

}
