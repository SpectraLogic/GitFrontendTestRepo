/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.query.WhereClause;
import com.spectralogic.util.db.service.BaseService;

import java.util.List;
import java.util.Set;
import java.util.UUID;

final class DataPersistenceRuleServiceImpl 
    extends BaseService< DataPersistenceRule > implements DataPersistenceRuleService
{
    DataPersistenceRuleServiceImpl()
    {
        super( DataPersistenceRule.class );
    }

    public List<DataPersistenceRule> getRulesToWriteTo(final UUID dataPolicyId, final IomType jobRestore )
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
                        DataPlacement.DATA_POLICY_ID,
                        dataPolicyId ),
                ruleTypeFilter ) ).toList();
    }
}
