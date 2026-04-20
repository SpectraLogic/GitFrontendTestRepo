/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.notification.domain;


public interface SequencedNotificationPayload<T extends SequencedEvent> extends NotificationPayload
{
    String LAST_PROCESSED_EVENT = "lastProcessedEvent";

    Long getLastProcessedEvent();

    void setLastProcessedEvent(final Long value);

    String CHANGES = "changes";

    T[] getChanges();

    void setChanges(final T[] value);
}