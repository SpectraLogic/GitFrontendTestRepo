/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.BaseService;

final class StorageDomainMemberServiceImpl
    extends BaseService< StorageDomainMember > implements StorageDomainMemberService
{
    StorageDomainMemberServiceImpl()
    {
        super( StorageDomainMember.class );
    }
    

    public void ensureWritePreferencesValid()
    {
        verifyInsideTransaction();
        final Map< UUID, TapePartition > tapePartitions = BeanUtils.toMap( 
                getServiceManager().getRetriever( TapePartition.class ).retrieveAll().toSet() );
        for ( final StorageDomainMember member : retrieveAll(
                Require.not( Require.beanPropertyEquals( 
                        StorageDomainMember.WRITE_PREFERENCE, WritePreferenceLevel.NEVER_SELECT ) ) )
                        .toSet() )
        {
            final TapePartition tapePartition = tapePartitions.get( member.getTapePartitionId() );
            if ( null == tapePartition || null == tapePartition.getDriveType() )
            {
                continue;
            }
            
            if ( !tapePartition.getDriveType().isWriteSupported( member.getTapeType() ) )
            {
                update( member.setWritePreference( WritePreferenceLevel.NEVER_SELECT ),
                        StorageDomainMember.WRITE_PREFERENCE );
                getServiceManager().getService( StorageDomainFailureService.class ).create( 
                        member.getStorageDomainId(),
                        StorageDomainFailureType.MEMBER_BECAME_READ_ONLY,
                        "Cannot write to " + member.getTapeType() + " tapes in partition " +
                                tapePartition.getSerialNumber() + ".  Marked storage domain member as read-only.",
                        null );
            }
        }
    }
    
    
    @Override
    public void create( final StorageDomainMember bean )
    {
        setAutoCompactionThreshold( bean );
        super.create( bean );
    }
    
    
    @Override
    public void create( final Set< StorageDomainMember > beans )
    {
        for ( final StorageDomainMember bean : beans )
        {
            setAutoCompactionThreshold( bean );
        }
        super.create( beans );
    
    }


    @Override
    public Set< StorageDomainMember > getStorageDomainMembersToWriteTo(final UUID dataPolicyId, final IomType jobRestore )
    {
        final WhereClause ruleTypeFilter;
        if (jobRestore == IomType.STAGE) {
            //This is a stage job
            ruleTypeFilter = Require.beanPropertyEquals(DataPersistenceRule.TYPE, DataPersistenceRuleType.TEMPORARY);
        } else if (jobRestore == IomType.STANDARD_IOM) {
            //This is a non-stage IOM job
            ruleTypeFilter = Require.beanPropertyEquals(DataPersistenceRule.TYPE, DataPersistenceRuleType.PERMANENT);
        } else {
            //This is a standard job
            ruleTypeFilter = Require.not( Require.beanPropertyEquals(
                    DataPersistenceRule.TYPE,
                    DataPersistenceRuleType.RETIRED ));
        }
        return retrieveAll( Require.all(
                Require.beanPropertyEquals(
                        StorageDomainMember.STATE,
                        StorageDomainMemberState.NORMAL ),
                Require.not( Require.beanPropertyEquals(
                        StorageDomainMember.WRITE_PREFERENCE,
                        WritePreferenceLevel.NEVER_SELECT ) ),
                Require.exists(
                        StorageDomainMember.STORAGE_DOMAIN_ID,
                        Require.exists(
                                DataPersistenceRule.class,
                                DataPersistenceRule.STORAGE_DOMAIN_ID,
                                Require.all(
                                        Require.beanPropertyEquals(
                                                DataPlacement.DATA_POLICY_ID,
                                                dataPolicyId ),
                                        ruleTypeFilter ) ) ) ) )
                .toSet();
    }

    private void setAutoCompactionThreshold( final StorageDomainMember bean )
    {
        if ( null != bean.getTapeType() )
        {
            if ( null == bean.getAutoCompactionThreshold() )
            {
                bean.setAutoCompactionThreshold( bean.getTapeType().getDefaultAutoCompactionThreshold() );
            }
        }
    }
}
