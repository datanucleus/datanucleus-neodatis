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
 * Representation of a NeoDatis "Native" query for use in DataNucleus.
 * Created by passing in the NativeQuery object.
 */
public class NativeQuery extends AbstractJavaQuery
{
    /** The NeoDatis native query. */
    org.neodatis.odb.core.query.nq.NativeQuery query = null;

    /**
     * Constructs a new query instance that uses the given persistence manager.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     */
    public NativeQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, null);
    }

    /**
     * Constructor for a query using NeoDatis "native" query language.
     * The final parameter must implement "org.neodatis.odb.core.query.nq.NativeQuery".
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param nativeQuery The native query
     * @throws NucleusUserException When the second parameter isn't an implementation of a Query
     */
    public NativeQuery(StoreManager storeMgr, ExecutionContext ec, Object nativeQuery)
    {
        super(storeMgr, ec);

        if (!(nativeQuery instanceof org.neodatis.odb.core.query.nq.NativeQuery))
        {
            throw new NucleusUserException(Localiser.msg("NeoDatis.Native.NeedsQuery"));
        }

        this.query = (org.neodatis.odb.core.query.nq.NativeQuery)nativeQuery;
        this.candidateClassName = query.getObjectType().getName();
    }

    public void compileGeneric(Map parameterValues)
    {
    }

    /**
     * Method to compile the query.
     * We have nothing to compile with NeoDatis native queries since all is done at execution.
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
                NucleusLogger.QUERY.debug(Localiser.msg("021046", "Native", getSingleStringQuery()));
            }

            // Try to find any comparator info
            Objects resultSet = null;
            try
            {
                resultSet = odb.getObjects(query);
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException("Exception thrown querying with native query", e);
            }

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021074", 
                    "Native", "" + (System.currentTimeMillis() - startTime)));
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
        return "NeoDatis Native Query <" + candidateClassName + ">";
    }
}