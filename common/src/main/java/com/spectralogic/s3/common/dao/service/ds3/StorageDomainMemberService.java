/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.IomType;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.util.db.service.api.BeanCreator;
import com.spectralogic.util.db.service.api.BeanDeleter;
import com.spectralogic.util.db.service.api.BeanUpdater;
import com.spectralogic.util.db.service.api.BeansRetriever;

import java.util.Set;
import java.util.UUID;

public interface StorageDomainMemberService 
    extends BeansRetriever< StorageDomainMember >,
            BeanCreator< StorageDomainMember >, 
            BeanUpdater< StorageDomainMember >,
            BeanDeleter
{
    void ensureWritePreferencesValid();

    Set< StorageDomainMember > getStorageDomainMembersToWriteTo(final UUID dataPolicyId, final IomType jobRestore);
}
