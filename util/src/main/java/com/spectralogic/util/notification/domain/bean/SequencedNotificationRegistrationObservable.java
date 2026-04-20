package com.spectralogic.util.notification.domain.bean;

import com.spectralogic.util.bean.lang.Optional;

public interface SequencedNotificationRegistrationObservable< T extends SequencedNotificationRegistrationObservable< T >>
            extends HttpNotificationRegistration< T >
{
    @Optional
    Long getLastSequenceNumber();

    T setLastSequenceNumber(final Long value );

    String LAST_SEQUENCE_NUMBER = "lastSequenceNumber";
}
