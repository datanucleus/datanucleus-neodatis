/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
 **********************************************************************/
package org.datanucleus.store.neodatis.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.neodatis.NeoDatisUtils;
import org.datanucleus.store.query.AbstractJavaQuery;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.neodatis.odb.ODB;
import org.neodatis.odb.Objects;

/**
 * Representation of a NeoDatis "Criteria" query for use in DataNucleus.
 * Created by passing in the CriteriaQuery object.
 */
public class CriteriaQuery extends AbstractJavaQuery
{
    /** The NeoDatis criteria query. */
    org.neodatis.odb.impl.core.query.criteria.CriteriaQuery query = null;

    /**
     * Constructs a new query instance that uses the given persistence manager.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     */
    public CriteriaQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, null);
    }

    /**
     * Constructor for a query using NeoDatis "native" query language.
     * The second parameter must implement "org.neodatis.odb.impl.core.query.criteria.CriteriaQuery".
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param criteriaQuery The criteria query
     * @throws NucleusUserException When the second parameter isn't an implementation of a Query
     */
    public CriteriaQuery(StoreManager storeMgr, ExecutionContext ec, Object criteriaQuery)
    {
        super(storeMgr, ec);

        if (!(criteriaQuery instanceof org.neodatis.odb.impl.core.query.criteria.CriteriaQuery))
        {
            throw new NucleusUserException(Localiser.msg("NeoDatis.Criteria.NeedsQuery"));
        }

        this.query = (org.neodatis.odb.impl.core.query.criteria.CriteriaQuery)criteriaQuery;
        this.candidateClassName = query.getFullClassName();
    }

    /**
     * Method to compile the query.
     * We have nothing to compile with NeoDatis criteria queries since all is done at execution.
     */
    protected void compileInternal(Map parameterValues)
    {
    }

    /**
     * Method to return if the query is compiled.
     * @return Whether it is compiled
     */
    protected boolean isCompiled()
    {
        return true;
    }

    /**
     * Method to execute the query.
     * @param parameters Map of parameter values keyed by the name
     * @return The query result
     */
    protected Object performExecute(Map parameters)
    {
        ManagedConnection mconn = getStoreManager().getConnection(ec);
        ODB odb = (ODB) mconn.getConnection();
        try
        {
            // Execute the query
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021046", "Criteria", getSingleStringQuery()));
            }

            // Try to find any comparator info
            Objects resultSet = null;
            try
            {
                resultSet = odb.getObjects(query);
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException("Exception thrown querying with criteria query", e);
            }

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021074", 
                    "Criteria", "" + (System.currentTimeMillis() - startTime)));
            }

            // Assign ObjectProviders to any returned persistable objects
            ArrayList results = new ArrayList(resultSet);
            AbstractClassMetaData cmd = null;
            Iterator iter = results.iterator();
            while (iter.hasNext())
            {
                Object obj = iter.next();
                if (ec.getApiAdapter().isPersistable(obj))
                {
                    if (cmd == null)
                    {
                        cmd = ec.getMetaDataManager().getMetaDataForClass(getCandidateClassName(), 
                            ec.getClassLoaderResolver());
                    }
                    NeoDatisUtils.prepareNeoDatisObjectForUse(obj, ec, odb, cmd);
                }
            }

            return results;
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Convenience method to return whether the query should return a single row.
     * @return Whether a single row should result
     */
    protected boolean shouldReturnSingleRow()
    {
        // We always return the List of objects returned by the query since no other way of knowing if unique
        return false;
    }

    /**
     * Method to return the query as a single string.
     * @return The single string
     */
    public String getSingleStringQuery()
    {
        return "NeoDatis Criteria Query <" + candidateClassName + ">";
    }
}